# RiffDB Implementation Plan

## 1. Repository Assessment

### Current contents

The repository is now bootstrapped and contains the 29-crate Rust workspace,
quality and generation scripts, GitHub Actions configuration, ADR records,
Protobuf sources and compatibility fixtures, the bounded contract parser and its
corpus/fuzz target, and the original planning inputs and diagrams. WP-000 through
WP-030 have landed; accepted ADR-0014 through ADR-0016 require narrow follow-up
work in WP-010 and WP-030 before WP-040 begins.

ADR-0002, ADR-0003, ADR-0005, ADR-0006, ADR-0010, ADR-0011, and ADR-0013 through
ADR-0016 are Accepted. ADR-0001, ADR-0004, ADR-0007 through ADR-0009, and
ADR-0012 remain Proposed and are non-authoritative until their exact text receives
human approval.

### Git state

Git is initialized on `main`; the authoritative baseline is `3235477` and WP-000
landed as `4407d59`. No remote is configured yet. The maintainer has confirmed
that the repository will be pushed to GitHub, so GitHub Actions remains the CI
provider and adding the remote can wait until publication.

### Rust and host environment

The host is x86-64 Linux with the pinned Rust 1.97.0 toolchain, rustfmt, Clippy,
Git, Graphviz, `cargo-deny`, `cargo-audit`, `cargo-machete`, and `cargo-fuzz`.
The pinned nightly used only for parser fuzzing is also installed. Tool versions
and CI installation policy are recorded by the workspace bootstrap.

### Existing conflicts

No existing implementation conflicts with the architecture are known. The
governance reconciliation aligns grammar, gRPC/value boundaries, gate membership,
package dependencies and paths, coordinator ownership, comparison tracks, server
composition, stable IR IDs, plan-hash domains, explicit binding failures, and
canonical partition/index keys. Section 2 lists the remaining package-level
freezes.

### Missing prerequisites for WP-000

None. WP-000 is complete. Its baseline, toolchain, dependency policy, CI provider,
workspace inventory, generated-artifact convention, and acceptance evidence were
frozen before semantic implementation began.

## 2. Consistency Findings

The reconciled manifest has 26 unique work-package IDs and an acyclic dependency
graph. Gate membership, SPEC package table, YAML dependencies, and the work-package
DAG agree. All referenced normative requirement IDs exist or are explicitly
classified by the manifest's coverage policy. The 29 crate names and three POC
binaries (`riffdbd`, `riffdb`, and `riffdb-mcp`) agree across the specification
and package scopes. Allowed paths now cover each declared deliverable and
acceptance script. Public/durable ownership and MCP/gRPC/CLI/SDK layering agree
with the shared-service and coordinator boundaries.

### Blocking before WP-000

None. All identified WP-000 blockers were resolved before commit `4407d59`.

### Blocking before P0

1. **Accepted key/hash additions need their owning WP-010 follow-up.**
   `PartitionKey`, `IndexEntryKey`, `PartitionKeyHash`, projection plan hashes,
   and contract plan-root hashes must land in `riffdb-types` before WP-040 or
   WP-060 consumes them.
2. **Accepted binding failures need their owning WP-030 follow-up.** Every
   `read`, `mutate`, and `create` binding must require an explicit `else` outcome,
   and the canonical `CreateBudget` workload must enter the parser corpus before
   WP-040 freezes its bundle.
3. **ADR-0001, ADR-0004, and ADR-0009 remain Proposed.** ADR-0001's spec
   deadline is P0; ADR-0004/0009's exact storage transaction,
   semantic port, and capability-record boundaries must be accepted before
   WP-060 exposes public traits.
4. **ADR-0012 remains Proposed.** Logical-time admission and deterministic
   runtime context must be accepted before WP-080. The deterministic arithmetic
   and resource-fault durability/public-error disposition required by ADR-0013
   must be frozen at the same boundary.

### Blocking before P1

No additional authoritative-input contradiction remains. After the P0 ADR batch,
ADR-0007 must be accepted before WP-100/WP-120 freeze the shared application
service and coordinator entry points.

### Blocking before P2

No additional authoritative-input contradiction remains. ADR-0008 must be
accepted before WP-140 freezes MCP names/resources. WP-185 owns final server composition, WP-075 owns
the isolated Fjall comparison, and WP-140 owns its Inspector script.

### Non-blocking clarifications

- `riffdb-codegen` and `riffdb-replay` remain deliberately deferred. WP-000 creates
  no such crates; code generation may become a `riffdb contract generate`
  subcommand, while privileged replay/repair is reconsidered after the POC.
- WP-020 owns the stable Protobuf policy and common schema. Later semantic records
  use reviewed proto-owner interface PRs rather than speculative WP-020 fields.
- PostgreSQL and Fjall dependency/version choices are intentionally deferred to
  WP-045 and WP-075 human review. They remain in isolated nested workspaces.
- Capability cryptographic dependency selection and key-rotation mechanics remain
  part of the ADR-0009 acceptance review; no token implementation may choose them
  implicitly.

## 3. Planning Assumptions

1. `AGENTS.md`, `SPEC.md`, `work_packages.yaml`, and Accepted ADRs are
   authoritative. Proposed ADRs and diagrams are planning context only.
2. The reconciled YAML hard dependencies and allowed paths are preserved exactly.
   Soft sequencing below never removes a declared dependency.
3. All first-party crates are private Rust 2024 workspace packages on the fixed
   Rust 1.97.0 baseline. Linux CI is gating; macOS is best effort and Windows is
   outside the POC gate.
4. Redb is the production POC backend behind the semantic storage API. No redb
   type crosses into runtime, service, policy, or transport crates.
5. POC commands have no randomness. Logical time is fixed in durable admission
   and passed as an immutable transaction input.
6. Public service DTOs are transport-neutral. Prost, Tonic, rmcp, redb, and
   transport status types remain inside adapters.
7. JSON Schemas are compiler artifacts in deterministic contract bundles. MCP
   wraps them and never creates a competing semantic schema.
8. Every command terminal result and every authoritative catalog/capability
   change uses a typed commit-coordinator path. Administrative changes use a
   separate ordered audit record, not an application `CommitSequence`.
9. Generated source is checked in only where an owning package provides a
   deterministic `--check` generator. Empty placeholders are prohibited.
10. Empty directories are not committed. Each package materializes only paths
    containing real artifacts.
11. PostgreSQL and Fjall exist only in their isolated evidence workspaces. Their
    drivers, SQL, and engine dependencies never enter a `riffdb-*` production
    crate or the root lockfile.

## 4. Architectural Interface Map

The following map is the minimum interface freeze needed before agents branch.
Ownership and direction were approved on 2026-07-12 and are reflected in the
reconciled specification and manifest. An interface tied to a Proposed ADR still
cannot freeze until that ADR's exact text is Accepted.

| Interface | Owning crate / first producer | First consumers | Required stability and ADR | Freezing evidence |
|---|---|---|---|---|
| Stable IDs, canonical values, fixed-scale decimal/money, entity keys, logical timestamps | `riffdb-types`, WP-010 | WP-020, WP-030, WP-040, WP-060, WP-090, then all packages | Byte encoding, bounds, checked arithmetic, ordering, and cross-crate ID inventory stable before parallel work; ADR-0011 | Golden byte/hash vectors, decimal overflow/scale properties, ordering and round-trip tests, encoder fuzz |
| Public-safe errors and internal incident layering | `riffdb-errors`, WP-010 | All semantic crates, then adapters | Stable safe code/class/retry disposition and maximum detail; internal sources remain private; ADR-0006 only when persisted/wire encoded | Secret-canary/redaction properties, bounded snapshots, business-outcome-not-error assertions |
| Syntax AST, source map, parser diagnostics | `riffdb-contract-syntax`, WP-030 | WP-040 and diagnostics only | Grammar version and canonical budget source fixed; AST is neither durable nor runtime input; ADR-0002 | Valid/invalid corpus, source-span snapshots plus semantic assertions, bounded parser fuzz |
| Typed HIR, executable IR, command/read/invariant/projection plans, transport-neutral JSON Schemas | `riffdb-contract-ir`, WP-040; compiler produces instances | WP-050, WP-080, WP-130 codegen, WP-140, WP-170, WP-180 | Stable IDs, instruction validation, IR version, canonical plan-hash inputs before WP-050/WP-080; ADR-0002 | Golden budget IR/hash/schema/explain, repeated-build equality, unsupported-version rejection |
| Immutable `ContractBundle` | `riffdb-contract-ir`, WP-040 | `riffdb-catalog`, runtime/service/MCP by reference | Separate application version, compiler version, IR version, source hash, and plan hash; no `generated_at` in canonical content; ADR-0002/0006 | Canonical bundle fixture, compatibility fixture, deterministic regeneration |
| Active catalog snapshot, deployment CAS, catalog notifications | `riffdb-catalog`, WP-050 | WP-100, WP-120, WP-140 | Active pointer changes only after durable bundle through a typed coordinator administrative operation; exact old/new result, ordered audit, and notification timing; ADR-0002/0004/0005/0007 | Expected-version tests, old-or-new crash test, audit ordering, notification-after-durability test |
| `ReadSnapshot` | `riffdb-storage-api`, WP-060 | WP-080, WP-100, catalog/query services | Synchronous, bounded, engine-neutral reads; no storage write transaction held through runtime evaluation; ADR-0004 | Shared memory/redb conformance suite for consistent entity versions, absence, bounded prefix/epoch, ordered scans |
| Compile-time read/predicate templates | `riffdb-contract-ir`, WP-040 | WP-080 and WP-100 | Every influential read statically visible; predicate plan hash/version stable; ADR-0002/0003 | Compiler negative tests for hidden/unbounded reads and semantic dependency assertions |
| Observed read and predicate evidence | `riffdb-storage-api`, WP-060 | WP-080 materializes; WP-100 revalidates; WP-020 encodes compatible records through proto-owner PRs | Variants, current-value lookup, absence/version/epoch meaning, exact plan identity, and canonical order fixed before WP-080; ADR-0003/0004 | Mutation-between-evaluation-and-commit tests for every variant; undeclared dependency or plan mismatch fails closed |
| `CommitIntent` runtime output | Type owned by `riffdb-storage-api`, WP-060; instances constructed by `riffdb-runtime`, WP-080, as required by the SPEC | WP-100 and testkit | Admission supplies a value-only transaction context; runtime produces owned canonical mutations, event intents, outcome, dependencies, provenance values, and commit-check evidence. No lease, parser AST, request buffer, storage transaction, credential secret, or I/O handle; ADR-0003/0004/0005/0012 | Golden intent, reference-model differential histories, determinism properties, compile/API checks against live-capability serialization |
| `StorageEngine` and atomic command transaction | `riffdb-storage-api`, WP-060; memory/redb implement | WP-070 and WP-100 | Must be a narrow semantic transaction, not SQL, arbitrary callback, or generic mutation API. Coordinator retains exact order: idempotency check, dependency/predicate validation, sequence allocation, full atomic apply; ADR-0003/0004/0005 | Same conformance suite over memory/redb, exact pre/post failpoints, no visible partial state |
| `ConflictKey` canonical bytes | `riffdb-types`, WP-010 | WP-040 derives expressions, WP-090 acquires, WP-100 uses | Type prefix, component encodings, sorting and dedup stable before WP-090; ADR-0003/0011 | Golden keys and ordering properties, malformed/bound tests |
| `ConflictManager` and acquired mutation capability | `riffdb-conflict`, WP-090 | WP-100 only | RAII lease is non-Clone, non-Serialize, idempotently released, all-or-nothing for sorted keys, and never enters `CommitIntent`; ADR-0003 | Loom grant/cancel/release/fairness model, Shuttle multi-key schedules, cancellation/panic cleanup |
| Idempotency identity and canonical input hash | Components in `riffdb-types` WP-010; rules in `riffdb-idempotency` WP-100; opaque storage key/admission in `riffdb-storage-api` WP-060 | WP-070 table layout, WP-100, WP-120, transports | Database/environment + authorization-resolved tenant + stable principal + contract lineage + stable command ID + keyed caller-key digest; contract version stored but excluded from lookup; pending admission has input/plan/time and no sequence; ADR-0005/0011/0012 | Cross-scope isolation, golden digests, mismatch tests, deployment retry, reservation crash/resume tests |
| Stored outcome vs committed outcome | `StoredOutcome` in `riffdb-storage-api` WP-060; transport-neutral `CommittedOutcome` in `riffdb-commit` WP-100 | WP-100 and WP-120 respectively | Stored form is durable typed result plus original sequence/context. Committed form wraps it with current-call `replayed` metadata. Replay creates no new record or sequence; ADR-0005/0006 | Equal retry/mismatch fixtures, business rejection sequence test, lost-response process test |
| Actor, authenticated principal, capability, decision, obligations | IDs/claim values in `riffdb-types` WP-010; credential resolution in `riffdb-auth` WP-110; `Decision` and redaction/audit obligations in `riffdb-policy` WP-110 | WP-120 mandatory; WP-130/WP-140 map credentials and discovery | Opaque 32-byte token, base64url text, HMAC-SHA-256 digest with key ID/version, environment/database/audience binding, trusted-vs-untrusted claims, deny default, revocation point, and field/tenant/approval obligations; ADR-0007/0009 | HMAC vectors, raw-token absence, revocation/scope/audience tests, obligation and stale-name denial matrices, secret canaries |
| API-neutral service requests/results | `riffdb-service`, WP-120 | WP-130 and WP-140 | No Prost/Tonic/rmcp/redb types. Every operation takes authenticated context and obtains a policy decision; bounded cursor/wait/result types fixed before transport branches; ADR-0007 | In-process service suite and dependency/architecture test proving no transport-storage edge |
| Commit record, durable events, provenance | Semantic store records in `riffdb-storage-api` WP-060; versioned payloads/envelopes in `riffdb-proto` WP-020; WP-100 supplies values | WP-070, WP-160, WP-170, services | Field numbers, canonical repeated order, complete post-image policy, redaction, event IDs, atomic record set fixed before WP-070; ADR-0005/0006 | Golden old/current records, decoder fuzz, torn-write failpoints, provenance/idempotency assertions on every mutation |
| Capability repository | Neutral storage DTO/port in `riffdb-storage-api` WP-060, redb implementation WP-070; auth semantics WP-110 | WP-110/WP-120 | Token digest never exposed; create/revoke use typed coordinator operations and separately ordered administrative audit; ADR-0007/0009 | Redb/memory conformance, raw-token absence, audit ordering, revocation-next-request test |
| Outbox repository and transition | Neutral event/delivery storage port in `riffdb-storage-api` WP-060, redb WP-070; worker semantics WP-160 | WP-160 and recovery | Atomic event intent with command; stable event ID; lease/retry transitions bounded; ADR-0005/0006 | Commit/outbox atomicity, fake connector duplicate/crash histories, no-lost-event scan |
| Projection plan, state operation, and frontier | Plan in `riffdb-contract-ir` WP-040; atomic store port in `riffdb-storage-api` WP-060/redb WP-070; lifecycle/query semantics in `riffdb-projection` WP-170 | WP-120 service port, WP-140 resources | State plus frontier atomicity, `(projection, sequence)` idempotency, contiguous prefix and typed waits fixed; ADR-0010 | Prefix/model properties, apply/frontier failpoints, rebuild and wait tests |
| Static gRPC schema boundary | `.proto` and generated wire types in `riffdb-proto`, WP-020; later records through proto-owner interface PRs; adapter mapping in WP-130 | WP-130/client/server | SPEC Section 11's five services and descriptive RPCs are canonical; exact tagged `riffdb.v1.Value` replaces `Struct`; wire types never become service DTOs; ADR-0006/0007/0011 | Field/service golden descriptors, exact-value vectors, generated clean diff, gRPC conformance and limit/error tests |
| Generated MCP boundary | Contract IR owns schemas/metadata; `riffdb-api-mcp` WP-140 owns rmcp tools/resources/names/URIs/text shaping; WP-185 composes HTTP | MCP transports only | `riffdb.cmd.<normalized_contract>.<normalized_command>`, deployment-time collision rejection, configured resource/audience URI, policy-filtered discovery and invocation-time reauthorization; ADR-0008/0009 | Golden tool/resource definitions, collision tests, structured result schema validation, both-transport auth/conformance tests |

### Proposed internal flow

The approved direction, subject to exact acceptance of the referenced Proposed
ADRs, is:

```text
transport decode/authenticate
  -> riffdb-service authorizes and resolves active plan
  -> riffdb-commit resolves or durably creates Pending admission with fixed tx.time
  -> riffdb-commit acquires conflict lease and materializes the bounded read set
  -> riffdb-runtime synchronously evaluates owned values and constructs CommitIntent
  -> coordinator drives one narrow storage transaction
  -> storage atomically persists mutations + outcome + events + provenance + commit
  -> service applies output obligations
  -> transport maps the safe result
```

The service owns API admission and policy. The commit crate owns transaction admission from the first idempotency check through lease release and the committed result. Runtime owns pure plan evaluation and construction of the self-contained intent type defined below it. Storage owns mechanics, not sequence policy or business semantics. MCP and gRPC own only protocol adaptation.

## 5. Dependency and Parallelization Strategy

### Hard-dependency waves

The following preserves every reconciled YAML dependency:

1. **Wave A:** WP-000.
2. **Wave B:** WP-010.
3. **Wave C:** WP-020, WP-030, and WP-090 in parallel after their required ADRs and WP-010 interfaces freeze.
4. **Wave D:** WP-040 after WP-030 and WP-060 after WP-020. They may overlap after the read-template/evidence boundary is reviewed.
5. **Wave E:** WP-045 after WP-040; WP-050 after WP-020/WP-040/WP-060; WP-070 after WP-020/WP-060; and WP-080 after WP-040/WP-060. These may run in parallel within their isolated paths.
6. **Wave F:** WP-075 and WP-110 after WP-070. The Fjall workspace remains isolated; WP-110 consumes the neutral capability store.
7. **Wave G:** WP-100 after WP-050/WP-070/WP-080/WP-090/WP-110.
8. **Wave H:** WP-120 after WP-050/WP-100/WP-110.
9. **Wave I:** WP-125, WP-130, WP-160, WP-170, and WP-180 may run in parallel after their declared dependencies. WP-125 also needs WP-045.
10. **Wave J:** WP-135 after WP-125/WP-130; WP-140 after WP-040/WP-120/WP-130; and WP-150 after WP-130.
11. **Wave K:** WP-185 after WP-130/WP-140/WP-160/WP-170/WP-180.
12. **Wave L:** WP-190 after WP-070/WP-100/WP-160/WP-170/WP-185.
13. **Wave M:** WP-200 after its complete declared evidence set, including WP-075, WP-135, WP-185, and WP-190.

### Interface-first merge rules

- WP-010 lands canonical types/errors and fixtures before WP-020, WP-030, or WP-090 branches. No package may duplicate a semantic newtype.
- WP-040 lands `riffdb-contract-ir` interfaces before completing compiler passes; WP-080 consumes only merged IR.
- WP-060 lands reviewed snapshot, observed-dependency, atomic-transaction, catalog, capability, outbox, and projection ports before memory/redb implementations branch.
- WP-100 lands `CommittedOutcome` and its coordinator handle before WP-120 begins.
- WP-120 lands service DTOs/traits before gRPC and MCP adapter branches.
- WP-045 freezes the backend-neutral budget workload/oracle before WP-125 or
  WP-135 edits the isolated comparison workspace.
- Shared changes to canonical values, IR, durable records, storage ports, policy decisions, service DTOs, `.proto`, or MCP naming require a small interface PR and coordinated rebase. Parallel agents must not edit the same owner crate to reconcile locally invented shapes.
- WP-070, WP-100, WP-160, and WP-170 must land named test-only failpoint hooks as part of their own acceptance, because WP-190 cannot retrofit them.
- Every earlier package emits redaction-safe telemetry through a minimal stable hook or tracing contract; WP-180 supplies aggregation/rendering rather than rewriting closed transaction code.

### Gate interpretation

P0 closes only after WP-030/WP-040/WP-060/WP-080 and transitive dependencies pass deterministic plan/model evidence. P1 includes WP-150 and proves the durable standalone command path, CLI, atomic outbox intent, and transaction-path failpoints; external delivery and the cross-component crash matrix remain P2. P2 includes WP-185 composition and closes only when POC-001 through POC-010 and all declared package evidence pass. WP-045/WP-075/WP-125/WP-135 are evidence tracks consumed by WP-200, not alternate semantic gates.

### Long-Lived Budget Comparison Application

Create one small annual-budget application that runs the same application-level scenarios against PostgreSQL and RiffDB. The reconciled manifest gives this long-lived evidence track explicit ownership in WP-045, WP-125, and WP-135; WP-150 packages its developer flow and WP-200 consumes its runner. It is not an alternate storage path or a production dependency.

**Boundary:** The comparison shares a workload manifest, normalized requests/outcomes, and a reference oracle, not database internals or a least-common-denominator storage trait. PostgreSQL code remains comparison-only. No RiffDB production crate depends on the example, a PostgreSQL driver, or SQL. The RiffDB adapter calls only the shared application service initially and the public SDK/gRPC path once available; it never imports runtime, commit, storage API, or redb.

**Repository shape as packages materialize it:**

```text
examples/budget-comparison/
  Cargo.toml
  Cargo.lock
  README.md
  core/
  fixtures/
  postgres/
  riffdb-service/
  riffdb-grpc/
  tests/
```

Keep this as one isolated private Rust example package, not another RiffDB semantic crate. Give it a nested standalone workspace and lockfile so PostgreSQL dependencies do not enter the root workspace lockfile or ordinary RiffDB builds; dedicated CI must still enforce Rust 1.97.0, workspace-equivalent lints, `#![forbid(unsafe_code)]`, formatting, Clippy, tests, and dependency policy. Select and review its PostgreSQL driver when the scoped work package begins; do not preselect a dependency in WP-000. Pin the PostgreSQL server version used in CI/benchmarks. SQL files are permitted only inside this comparison boundary and are outside the POC critical path.

**Phased delivery:**

1. **WP-045 after WP-040:** Freeze the canonical annual-budget workload, normalized observation format, guarantee profiles, and reference oracle. Implement the PostgreSQL schema/adapter and deterministic correctness cases. This is the early comparison foundation, not yet a claim of two-backend application parity.
2. **WP-125 after WP-120:** Add the first valid RiffDB adapter through `riffdb-service` and run the same parameterized semantic conformance suite against both implementations. An earlier adapter would bypass the required service/authorization boundary.
3. **WP-135 after WP-130:** Make the Rust SDK over gRPC the canonical RiffDB application adapter. Retain the in-process service adapter only as a labeled component/correctness mode.
4. **WP-150:** Package the same runner as a simple developer-facing example and demo flow; do not fork application logic into the CLI.
5. **WP-200:** Reuse the unchanged runner/workload definitions for reproducible process benchmarks and the comparison report.

**Semantic parity:** The initial guarantee profile covers exact fixed-scale money, budget creation/seeding through an allowed typed operation, positive allocation, `Allocated`/`InsufficientBudget`/invalid-input observations, final `allocated <= approved`, and two barrier-synchronized 80-unit allocations against 100 yielding exactly one success and final allocation 80. As later profiles are enabled, PostgreSQL must manually implement every guarantee being compared: atomic state/outcome/event/provenance, same-key/same-input replay, same-key/different-input rejection, and uncertain-response recovery. Authorization, external outbox delivery, or projection-frontier parity must not be claimed until both adapters and the workload explicitly implement and verify it.

**Benchmark policy:** Correctness conformance is a preflight for timing. Report in-process/component and realistic process results separately. The process comparison is `riffdbd` plus Rust SDK/gRPC versus PostgreSQL plus its driver. Match dataset, request stream, client concurrency, durability, reset/warmup/timed regions, and connection-pool policy; record PostgreSQL version, isolation, synchronous-commit settings, and all SPEC Section 17.8 metadata. Label any unequal transport, durability, or guarantee profile. The comparison has no TPS gate and PostgreSQL is not the semantic oracle.

**CI tiers:** Compile/unit checks run on every comparison-app change. Semantic conformance uses a pinned PostgreSQL service container with an explicit readiness protocol and bounded retries, never sleeps. Long process benchmarks run manually or on a controlled nightly runner and do not gate correctness PRs on noisy performance thresholds.

**PR boundaries:** WP-045 is the workload/oracle/PostgreSQL PR; WP-125 is the service-adapter/conformance PR; WP-135 is the SDK/gRPC adapter PR; WP-150 packages the demo; WP-200 adds benchmark scenarios/reporting. Each PR names its guarantee profile and must not edit shared RiffDB types to accommodate the comparison.

## 6. Work-Package Plan

Each package below preserves the reconciled YAML hard dependencies and allowed
paths. Required ADRs must be Accepted before an affected package freezes or
implements their interfaces.

### 6.1 Repository Bootstrap

#### WP-000 — Repository foundation

- **Purpose:** Establish the reproducible workspace, exact toolchain, safe-Rust policy, CI, dependency policy, ADR workflow, PR contract, and generation-check convention.
- **Hard dependencies:** None. A baseline repository revision and provisioned acceptance environment are operational prerequisites.
- **Upstream inputs:** The authoritative repository inputs and diagrams; no semantic interface.
- **Downstream interfaces:** Exact package/binary names, workspace lint inheritance, feature policy, ADR status/template, CI command set, and generator registration/check convention.
- **Principal risks:** Creating fake APIs, adding speculative dependencies, an uncompilable skeleton, unpinned CI tools, or claiming clean-checkout evidence from an unborn repository.
- **Acceptance evidence:** All YAML commands including requirement-coverage checking, workspace all-features tests, docs with warnings denied, generator clean-diff, and a clean fresh-checkout run.
- **Human-review triggers:** Scope amendment, dependency-license policy, any dependency/unsafe/native code, any semantic placeholder, or inability to pin the required tools.
- **Parallelism:** None; all later packages consume it.
- **Recommended PR boundary:** One bootstrap PR with separate commits for workspace targets, policy/ADR/scripts, and CI. Do not merge a partial foundation that downstream packages must work around.

### 6.2 P0 — Executable Semantics

#### WP-010 — Core types and error taxonomy

- **Purpose:** Define the one canonical set of identifiers, bounded values, decimal/money, logical time, key bytes, hashing primitives, and safe error classifications.
- **Hard dependencies:** WP-000.
- **Upstream inputs:** Workspace safety/lint policy and accepted ADR-0011/identity decisions.
- **Downstream interfaces:** `riffdb-types` newtypes/value semantics and `riffdb-errors` safe error envelope; an inventory must include every cross-crate ID shown in the SPEC, not only the illustrative list.
- **Principal risks:** Platform-dependent bytes, decimal ambiguity, Unicode ambiguity, hash-domain collision, unbounded values, secrets in errors, and later packages duplicating actor/event/index IDs.
- **Acceptance evidence:** Package tests/Clippy plus golden encoding/hash vectors and property tests for ordering, bounds, checked arithmetic, scale, and redaction.
- **Human-review triggers:** Any durable encoding, hash algorithm/domain, identifier representation, timestamp meaning, public error class, or persisted actor/provenance field.
- **Parallelism:** None until the interface commit merges.
- **Recommended PR boundary:** First PR/commit for newtypes and error interface; second for canonical encoding/decimal plus fixtures and properties.

#### WP-020 — Protobuf and durable envelope

- **Purpose:** Define reviewed v1 public/durable schemas and a pure-Rust reproducible generation pipeline.
- **Hard dependencies:** WP-010.
- **Upstream inputs:** Canonical values/errors, canonical gRPC service decision, durable-record inventory, ADR-0006.
- **Downstream interfaces:** `.proto` sources, generated `riffdb-proto` types, bounded decoder/conversion rules, envelope version/checksum/schema policy, reserved fields, and golden descriptors/records.
- **Principal risks:** Prematurely freezing later semantics, conflating wire and storage compatibility, lossy exact decimals, unknown-field loss, or allowing generated types into core services.
- **Acceptance evidence:** `cargo test -p riffdb-proto`, exact `generate-proto --check`, descriptor/old-record fixtures, size-limit tests, and decoder fuzz target registration for later fuzz packages.
- **Human-review triggers:** Any public or durable field number, record type, envelope checksum/hash meaning, compatibility class, or generator dependency.
- **Parallelism:** Safe with WP-030 and WP-090 after WP-010 freezes.
- **Recommended PR boundary:** Schema/interface review first; generator and conversions second; compatibility fixtures last.

#### WP-030 — Contract syntax

- **Purpose:** Implement the selected bounded v0.1 lexer, grammar, spans, syntax AST, diagnostics, corpus, and parser fuzzing.
- **Hard dependencies:** WP-010.
- **Upstream inputs:** Canonical scalar vocabulary/error bounds, SPEC Section 7.2 as the canonical v0.1 grammar, and exact acceptance of ADR-0002.
- **Downstream interfaces:** Spanned AST/source map and structured syntax diagnostics for WP-040. Neither is durable or executable.
- **Principal risks:** Accidentally accepting the old appendix pseudocode, general-language scope growth, parser panics, unbounded recursion/diagnostics, or spans without semantic assertions.
- **Acceptance evidence:** Valid/invalid corpus, budget parse, stable bounded diagnostic snapshots with semantic checks, and 30-second production-limit fuzz smoke.
- **Human-review triggers:** Any grammar change; loops, recursion, callbacks, host escape, I/O, dynamic dispatch, unbounded constructs, or syntax that hides dependencies.
- **Parallelism:** Safe with WP-020/WP-090 only after grammar decision and WP-010 freeze.
- **Recommended PR boundary:** Grammar/AST contract; lexer/parser; diagnostics/corpus; fuzz harness after its path is approved.

#### WP-040 — Typed IR and compiler

- **Purpose:** Resolve and type-check syntax into deterministic versioned IR, complete dependency/conflict/invariant/outcome/event/projection plans, schemas, compatibility, and explain output.
- **Hard dependencies:** WP-010 and WP-030.
- **Upstream inputs:** Frozen AST/source spans, canonical values/IDs, ADR-0002, and the decision for deterministic bundle metadata.
- **Downstream interfaces:** `ContractBundle`, typed HIR, forward-only `CommandPlan`, plan/hash/version metadata, schema IR, and read/predicate templates.
- **Principal risks:** Unstable numeric IDs/hash, AST evaluation at runtime, hidden dependencies, unsupported invariant acceptance, transport-specific metadata, and nondeterministic collections/timestamps.
- **Acceptance evidence:** Stable budget IR/plan hash/JSON Schemas/docs, compatibility and explain snapshots, invalid-corpus diagnostics, repeated-build equality, and generator clean check.
- **Human-review triggers:** New language control flow/invariant class, stable ID rule, IR/durable change, conflict/locality rule, hidden read, or plan-hash input change.
- **Parallelism:** May overlap WP-060 after the shared template/evidence split is reviewed.
- **Recommended PR boundary:** IR interface and fixtures; compiler passes; dependency/conflict/invariant analysis; schema/explain/generation.

#### WP-045 — Budget comparison baseline

- **Purpose:** Establish the long-lived annual-budget workload, normalized observation oracle, explicit guarantee profiles, and isolated PostgreSQL implementation.
- **Hard dependencies:** WP-040.
- **Upstream inputs:** Canonical budget contract/IR semantics, exact values, outcome definitions, and deterministic fixtures.
- **Downstream interfaces:** Backend-neutral workload and observations, golden oracle fixtures, PostgreSQL adapter/schema, and correctness preflight consumed by WP-125/WP-135/WP-200.
- **Principal risks:** Treating PostgreSQL as the oracle, leaking SQL/driver dependencies into RiffDB, comparing unequal guarantees, or timing an incorrect implementation.
- **Acceptance evidence:** The nested workspace test command, deterministic fixture regeneration, and a report of PostgreSQL version, isolation, durability, and concurrency assumptions.
- **Human-review triggers:** PostgreSQL driver/server dependency, SQL outside the isolated workspace, guarantee-profile change, workload semantic change, or a shared abstraction that resembles storage CRUD.
- **Parallelism:** Safe with WP-050/WP-070/WP-080 after WP-040; it must not edit root workspace semantics.
- **Recommended PR boundary:** One isolated workload/oracle/PostgreSQL PR; do not include any RiffDB adapter yet.

#### WP-060 — Storage semantic API

- **Purpose:** Define the narrow engine-neutral snapshot, observed-dependency, atomic command, catalog, capability, outbox, projection, scan, integrity, and backup semantics plus a memory reference engine.
- **Hard dependencies:** WP-010 and WP-020.
- **Upstream inputs:** Canonical/store record types, envelopes, and accepted transaction/storage ADRs.
- **Downstream interfaces:** `ReadSnapshot`, ordered commit scan, store-facing atomic request/result records, specialized repository ports, integrity report, and shared conformance suite.
- **Principal risks:** Giving storage sequence/business ownership, holding a transaction across runtime evaluation, exposing generic transactions, redb leakage, circular `CommitIntent` dependency, or omitting later persistence needs.
- **Acceptance evidence:** One memory-engine suite proving snapshot consistency, bounded reads, atomicity, sequence/log ordering, idempotency semantics, catalog CAS, and model-state equivalence.
- **Human-review triggers:** Transaction ordering/lifetime, revalidation responsibility, key/record encoding, sequence allocation, generic mutation ability, or any path that resembles arbitrary transaction callbacks.
- **Parallelism:** May overlap WP-040 only after the interface split is approved; its interface must merge before WP-050/WP-070/WP-080 implementations branch.
- **Recommended PR boundary:** ADR-backed trait/record interface; memory implementation; shared semantic/model properties.

#### WP-080 — Deterministic command runtime

- **Purpose:** Synchronously interpret compiler-produced IR against a snapshot and emit a deterministic typed result with complete read evidence and canonical mutation/event intent.
- **Hard dependencies:** WP-040 and WP-060.
- **Upstream inputs:** Frozen plan, canonical value/logical-time rules, `ReadSnapshot`, observed-dependency types, and the no-randomness decision.
- **Downstream interfaces:** Pure evaluator, value-only transaction context, construction of the approved lower-level `CommitIntent`, deterministic invariant evaluator, and generated-history adapters.
- **Principal risks:** Hidden clock/random/global state, `.await` or I/O, missed reads/predicates, noncanonical mutation/event ordering, input-buffer references, or divergent reference semantics.
- **Acceptance evidence:** Runtime/invariant unit tests, same-input/snapshot/time determinism, generated property histories, and reference-model differential comparison.
- **Human-review triggers:** Any randomness, time source, dynamic read, retry behavior, new instruction/control flow, commit-check representation, or invariant interpretation change.
- **Parallelism:** Safe with WP-050/WP-070 once WP-040/WP-060 interfaces merge.
- **Recommended PR boundary:** Evaluator/context interface; expression/instruction engine; dependency/invariant handling; differential histories.

#### P0 gate evidence

P0 closes only after identical contract inputs produce identical semantic IR, plan hashes, JSON Schemas, evaluated results, and reference-model histories. “Generated MCP metadata” at this gate means transport-neutral command metadata and schemas; protocol-specific MCP objects remain WP-140.

### 6.3 P1 — Durable Standalone Database

#### WP-050 — Contract catalog

- **Purpose:** Persist immutable bundles and atomically activate compatible expected versions with post-durability notifications.
- **Hard dependencies:** WP-020, WP-040, and WP-060.
- **Upstream inputs:** Versioned bundle encoding, compatibility report, catalog repository/CAS semantics, and typed coordinator administrative-operation boundary.
- **Downstream interfaces:** Active catalog snapshot, deployment request/result, immutable lookup by version/hash, expected-version behavior, and notification stream.
- **Principal risks:** Torn bundle/pointer, destructive compatibility, active plan/hash mismatch, notification before durability, or bypassing coordinator policy.
- **Acceptance evidence:** Catalog tests, expected-version races, compatibility fixtures, and old-or-new activation recovery. Process-kill durability must use a durable backend when available.
- **Human-review triggers:** Active-pointer ordering, bundle/storage format, destructive change, catalog sequencing, or notification visibility.
- **Parallelism:** Can overlap WP-070/WP-080 after WP-060 interface; cannot claim durable crash proof before redb support.
- **Recommended PR boundary:** Catalog interfaces/results; compatibility/activation; persistence/recovery; notifications.

#### WP-070 — Redb storage engine

- **Purpose:** Implement all frozen semantic storage ports using redb, versioned tables, atomic command records, recovery/integrity, offline backup/restore, and component benchmarks.
- **Hard dependencies:** WP-020 and WP-060.
- **Upstream inputs:** Frozen storage traits/records/envelopes/key layouts, durability modes, and named failpoint protocol.
- **Downstream interfaces:** Concrete engine/configuration hidden behind storage API, recovery/integrity report, backup manifest, and failpoint hooks for WP-190.
- **Principal risks:** Redb type leakage, sequence visibility before durability, incomplete specialized tables, format ambiguity, unsafe repair, or engine-specific semantics becoming public.
- **Acceptance evidence:** Shared engine conformance, package properties, process recovery matrix over table/metadata boundaries, verified backup/restore, and benchmark compilation.
- **Human-review triggers:** Table/key/envelope change, repair behavior, durability interpretation, backup overwrite policy, critical/unsafe/native dependency.
- **Parallelism:** Safe with WP-050/WP-080 after interface freeze.
- **Recommended PR boundary:** Layout/envelope fixtures; read/atomic operations; catalog/capability/derived ports; recovery/backup; benchmarks.

#### WP-075 — Fjall semantic comparison

- **Purpose:** Run the unchanged storage semantic conformance and comparable benchmark harness against an isolated Fjall adapter for the POC engine review.
- **Hard dependencies:** WP-060 and WP-070.
- **Upstream inputs:** Frozen semantic storage API, shared conformance/model suite, redb recovery/benchmark report shape, and durable-format fixtures.
- **Downstream interfaces:** A non-production adapter, separate lockfile/dependency inventory, conformance results, and comparable evidence for WP-200.
- **Principal risks:** Weakening the suite for Fjall, linking Fjall into `riffdbd`, importing its dependencies into the root workspace, or interpreting an engine limitation as a semantic exception.
- **Acceptance evidence:** The exact nested-workspace test and bench-compilation commands plus explicit pass/fail results for every unchanged semantic case.
- **Human-review triggers:** Fjall dependency/version, native/unsafe code, any conformance exception, format workaround, or proposal to make it a production backend.
- **Parallelism:** Safe with WP-110 after WP-070; isolated paths and lockfile prevent production interference.
- **Recommended PR boundary:** One experiment PR containing adapter, unchanged-suite wiring, recovery evidence, dependency inventory, and benchmark harness.

#### WP-090 — Conflict manager

- **Purpose:** Provide canonical, bounded, cancellation-safe multi-key exclusive capabilities and safe hot-key metrics.
- **Hard dependencies:** WP-010.
- **Upstream inputs:** Frozen `ConflictKey`, deadline/cancellation policy, safe error and hashed telemetry interfaces, ADR-0003.
- **Downstream interfaces:** `ConflictManager`, non-cloneable/non-serializable RAII mutation lease, deterministic test scheduler, queue/metric hooks.
- **Principal risks:** Double grant, lost wakeup, deadlock, cancellation/grant race, leaked lease, starvation, synchronous guard across `.await`, or raw key leakage.
- **Acceptance evidence:** Unit tests plus Loom grant/release/cancel/timeout/fairness/multi-key model and Shuttle larger schedules without sleeps.
- **Human-review triggers:** Fairness, all-or-nothing grant, ordering, lease lifetime, timeout classification, or conflict-key representation.
- **Parallelism:** Safe with WP-020/WP-030 after WP-010/ADR freeze.
- **Recommended PR boundary:** Reduced formal state model; production table/wait queues; cancellation/deadline/drop; diagnostics/stress.

#### WP-100 — Commit coordinator and idempotency

- **Purpose:** Own transaction admission after policy/plan resolution: idempotency, conflict acquisition, runtime evaluation, revalidation, sequence assignment, atomic persistence, replay, and lease release.
- **Hard dependencies:** WP-050, WP-070, WP-080, WP-090, and WP-110.
- **Upstream inputs:** Engine and atomic transaction, pure evaluator, conflict lease, catalog/commit-check plan, actor/provenance values, failpoints, and approved idempotency semantics.
- **Downstream interfaces:** Bounded coordinator handle/queue, idempotency derivation/reservation, `CommittedOutcome`, typed replay/mismatch/errors, subscriber notification after durability.
- **Principal risks:** Duplicate effects after uncertainty, wrong identity scope, unstable `tx.time`, sequence gaps/early visibility, missing predicate revalidation, business rejection mishandling, cancellation after commit starts, or lease release failure.
- **Acceptance evidence:** Package tests, bounded-backpressure tests, concurrent budget race, read-change revalidation, mismatch cases, and fail-after-durable-commit replay with exactly one mutation/event/record/sequence.
- **Human-review triggers:** Identity tuple/reservation, transaction ordering, predicate source, sequence semantics, atomic set, admission ownership, cancellation boundary, or outcome persistence.
- **Parallelism:** Starts only after WP-110; it may overlap no package that is still changing its catalog, storage, runtime, conflict, or principal inputs. Do not start WP-120 until its public result freezes.
- **Recommended PR boundary:** Identity/outcome interfaces; coordinator queue/state machine; atomic engine integration; concurrency/idempotency/failpoint evidence.

#### WP-110 — Authorization and capability service

- **Purpose:** Create and resolve opaque local capability tokens, enforce deny-by-default policy, support expiry/revocation, and produce redaction/tenant/field/approval/audit obligations.
- **Hard dependencies:** WP-010 and WP-070.
- **Upstream inputs:** Canonical actor/capability IDs, neutral capability repository, accepted HMAC/audience semantics, safe clock/secret handling, and typed coordinator administrative operations.
- **Downstream interfaces:** `AuthenticatedPrincipal`, `CapabilityRecord`, credential resolver, operation/capability request, `Decision`, obligations/redaction plan, and audit hook.
- **Principal risks:** Raw token persistence/logging, unreviewed crypto provider, cross-environment/audience replay, stale revocation, trusting client claims, or treating tool visibility as authorization.
- **Acceptance evidence:** Token secrecy, scope/environment/audience, expiry/revocation-next-request, tenant/field/approval, audit, visibility, and non-bypass tests.
- **Human-review triggers:** Cryptography/provider, token entropy/hash, administrative capability, approval, revocation point, audience, field policy, redaction, or persistence path.
- **Parallelism:** Safe with WP-075 after WP-070. WP-100 waits for its principal/capability interfaces.
- **Recommended PR boundary:** Token/store interface; authentication lifecycle; policy/obligations; cross-path security tests.

#### WP-120 — API-neutral application service

- **Purpose:** Expose one transport-free, policy-filtered command/contract/entity/commit/provenance/projection/health surface.
- **Hard dependencies:** WP-050, WP-100, and WP-110.
- **Upstream inputs:** Catalog, coordinator, auth/policy, query/storage ports, safe DTO/error types, and optional derived/health service ports.
- **Downstream interfaces:** Request context, service traits and implementations, bounded cursor/page/wait types, transport-neutral command/projection outcomes, and in-process harness.
- **Principal risks:** Transport or storage types leaking in, hidden generic mutation, authorization checked only once, obligations applied after output/logging, unbounded scans/waits, or policy-specific errors leaking secrets.
- **Acceptance evidence:** Service package tests, all-operation in-process end-to-end suite, and dependency architecture/non-bypass checks.
- **Human-review triggers:** Generic write/admin path, authorization placement, policy/result shape, pagination/cursor trust, projection wait semantics, or transport-specific behavior.
- **Parallelism:** None until its interface merges; afterward adapters/workers can branch.
- **Recommended PR boundary:** DTO/trait interface; command/contract/query implementation; pagination/waits; in-process architecture tests.

#### WP-125 — Service comparison adapter

- **Purpose:** Add the first valid RiffDB budget adapter through the API-neutral service and run the PostgreSQL workload/oracle without a storage bypass.
- **Hard dependencies:** WP-045 and WP-120.
- **Upstream inputs:** Stable workload/observations, PostgreSQL baseline, service DTOs, authorization setup, and committed outcome semantics.
- **Downstream interfaces:** Service-backed comparison adapter, parameterized cross-backend conformance, and correctness preflight retained for component analysis.
- **Principal risks:** Importing runtime/commit/storage, erasing RiffDB outcomes into CRUD, claiming parity for unimplemented guarantees, or changing shared types to fit the example.
- **Acceptance evidence:** The exact nested-workspace `service_comparison` command and matched normalized observations for every declared guarantee profile.
- **Human-review triggers:** New shared production interface, storage access, weakened oracle, added guarantee claim, or comparison-only dependency entering the root workspace.
- **Parallelism:** Safe with WP-130/WP-160/WP-170/WP-180 after WP-120; it edits only the isolated comparison workspace.
- **Recommended PR boundary:** One service-adapter and shared-conformance PR; no public SDK adapter yet.

#### WP-130 — gRPC server and Rust SDK

- **Purpose:** Map the approved public protocol onto the service, establish bounded `riffdbd` gRPC lifecycle/composition ports, and provide generic/generated Rust clients with safe same-key retry.
- **Hard dependencies:** WP-020 and WP-120.
- **Upstream inputs:** Frozen `.proto`, service DTOs, auth mapping, safe errors, server lifecycle/configuration, and later-worker composition ports.
- **Downstream interfaces:** Tonic services/interceptors/conversions, `riffdbd` composition API, transport client, ergonomic generated contract module, retry/error mapping.
- **Principal risks:** Business outcomes as status errors, adapter-side semantics, invented retry keys, protocol limits bypass, secrets in config, or missing hooks for MCP/outbox/projection/observability.
- **Acceptance evidence:** Package tests, malformed/limit/auth/status conformance, command/outcome/deploy/entity gRPC E2E, SDK retry tests, generated signature fixture, and every projection result variant mapped against an injected service stub.
- **Human-review triggers:** Public schema/status/retry change, auth interceptor, server configuration/privilege boundary, transport limit, or composition surface.
- **Parallelism:** May overlap WP-160/WP-170/WP-180 after composition ports; should precede WP-140 under the specified stdio design.
- **Recommended PR boundary:** Wire conversions/server; process composition/config; generic client; generated module/conformance.

#### WP-135 — Public SDK comparison adapter

- **Purpose:** Make the Rust SDK over public gRPC the canonical RiffDB adapter for the long-lived comparison runner.
- **Hard dependencies:** WP-125 and WP-130.
- **Upstream inputs:** Workload/oracle, service comparison baseline, public Rust client, gRPC authentication, retry/error/outcome mapping, and process lifecycle.
- **Downstream interfaces:** Public-only comparison adapter, same-key uncertain-response scenario, normalized process observations, and correctness preflight consumed by WP-200.
- **Principal risks:** Falling back to in-process internals, client-generated semantic behavior, benchmark transport mismatch, or divergence from WP-125 observations.
- **Acceptance evidence:** The exact nested-workspace `public_comparison` command, same-key recovery proof, and equality with normalized oracle observations before timing.
- **Human-review triggers:** Any non-public import, retry identity change, guarantee-profile change, benchmark topology change, or production API altered for the example.
- **Parallelism:** Safe with WP-140/WP-150 after WP-125 and WP-130; isolated files avoid adapter contention.
- **Recommended PR boundary:** One public SDK/gRPC adapter and process correctness PR.

#### WP-150 — CLI and local developer flow

- **Purpose:** Provide public-API-only operator/demo commands with stable machine JSON and additive human output.
- **Hard dependencies:** WP-130.
- **Upstream inputs:** Rust client, public auth/capability operations, error/retry semantics, configuration precedence, stable service operations, and budget comparison runner packaging contract.
- **Downstream interfaces:** `riffdb` command tree, bounded config/output model, local token workflow, and dry-run demo commands.
- **Principal risks:** Direct storage shortcut, secrets in argv/output/config, unstable JSON, unsafe retry, destructive restore defaults, or forking demo logic from the comparison runner.
- **Acceptance evidence:** CLI package tests, JSON snapshots/semantic assertions, public API integration, and `scripts/demo --dry-run`.
- **Human-review triggers:** Privileged bypass, credential input, destructive operation, stable JSON change, or a command unavailable through public API.
- **Parallelism:** Safe with WP-135/WP-140/WP-160/WP-170/WP-180 after WP-130.
- **Recommended PR boundary:** Command/config/output framework; contract/command/read/admin flows; comparison-runner demo wiring/tests.

#### P1 gate evidence

The durable gate must show one authoritative atomic command set, sequence/revalidation correctness, same-key uncertain-response recovery, deny-by-default shared service access, transaction-path process recovery, and the public-API-only CLI. Atomic outbox intent is part of the WP-070/WP-100 record set; external dispatch and the full outbox/projection crash matrix remain P2.

### 6.4 P2 — Native MCP and POC Exit

#### WP-140 — Native MCP interface

- **Purpose:** Expose authorized active-contract commands as dynamic tools and database context as filtered resources over stdio and loopback Streamable HTTP.
- **Hard dependencies:** WP-040, WP-120, and WP-130.
- **Upstream inputs:** Contract schemas/plans, service DTOs, policy/redaction decisions, gRPC client, name/URI decision, HTTP audience binding, and server registration hook.
- **Downstream interfaces:** Protocol-isolated rmcp adapter, session catalog, fixed tools/resources, structured result mapping, pagination/progress/cancellation, subscriptions/list-change behavior.
- **Principal risks:** Storage/service bypass, stale-name execution, schema mismatch, adapter-generated idempotency key, catalog overexposure, unsafe text, credential leakage, or HTTP binding outside loopback.
- **Acceptance evidence:** Package tests, golden authorized tool/resource definitions, structured schema validation, stale-name direct denial, init/list/page/progress/cancel/subscription tests, and Inspector smoke over both transports.
- **Human-review triggers:** Tool name/visibility/mutation, resource URI/content, authentication/audience, redaction/text shaping, protocol baseline, or server composition.
- **Parallelism:** May overlap WP-160/WP-170/WP-180 after WP-130’s needed interfaces. It is not safely parallel with the initial WP-130 interface work.
- **Recommended PR boundary:** Adapter/catalog and fixtures; stdio via gRPC; HTTP composition; protocol/security conformance.

#### WP-160 — Durable outbox

- **Purpose:** Dispatch atomically committed event intent outside command execution with explicit at-least-once behavior.
- **Hard dependencies:** WP-100 and WP-120.
- **Upstream inputs:** Stable event IDs/records, pending/delivery storage transitions, server worker port, redaction/audit hooks, and named failpoints.
- **Downstream interfaces:** Bounded scanner/lease loop, delivery state and backoff policy, connector trait, deterministic fake/test connector, optional disabled HTTP connector.
- **Principal risks:** Implying exactly-once, losing committed intent, hiding duplicate delivery, command-path I/O, unbounded response/error persistence, or nondeterministic tests.
- **Acceptance evidence:** State-machine properties, no-lost-event comparison with commit log, fake connector retries, process crash before/after destination acknowledgment, and visible duplicate scenario.
- **Human-review triggers:** Transaction/effect boundary, connector network/security dependency, delivery guarantee, event identity, retry clock, or stored external data.
- **Parallelism:** Safe with WP-140/WP-170/WP-180 after storage/composition interfaces.
- **Recommended PR boundary:** State/scanner; connector API/test connector; retry/duplicate/crash integration; optional HTTP separately.

#### WP-170 — Projection core

- **Purpose:** Maintain event-derived filters/counts/sums/groups with atomic monotonic frontiers and typed read-after-sequence results.
- **Hard dependencies:** WP-100 and WP-120.
- **Upstream inputs:** Projection plans, ordered commits, atomic state/frontier store, service port, worker composition, and failpoints.
- **Downstream interfaces:** Apply engine, durable lifecycle/frontier, query/wait interface, rebuild/degraded/invalid status, and safe notifications.
- **Principal risks:** Frontier ahead of state, skipped sequence, double application, stale result when `after_sequence` is unmet, source dependence on derived state, or non-rebuildable values.
- **Acceptance evidence:** Model prefix and monotonicity properties, `(projection, sequence)` idempotency, restart/rebuild, typed timeout/degraded cases, and both apply/frontier crash boundaries.
- **Human-review triggers:** Frontier atomicity/order, lifecycle/result semantics, new operator class, source-table projection, or any authoritative dependence on projection state.
- **Parallelism:** Safe with WP-140/WP-160/WP-180 after storage/composition interfaces.
- **Recommended PR boundary:** Operators/apply; frontier/query/wait; lifecycle/rebuild; process recovery properties.

#### WP-180 — Observability and diagnostics

- **Purpose:** Aggregate redaction-safe structured tracing, bounded metrics, health, and explain/operator diagnostics across existing paths.
- **Hard dependencies:** WP-120.
- **Upstream inputs:** Stable safe telemetry hooks/fields from earlier crates, component health ports, service plans/results, and server composition.
- **Downstream interfaces:** Subscribers/registry, health aggregation, diagnostic renderers, bounded label helpers, and secret-canary tests.
- **Principal risks:** Raw data reaches a subscriber before redaction, unbounded labels, derived failures misreported as rollback, inability to instrument closed crates, or diagnostics changing semantics.
- **Acceptance evidence:** Required field/metric presence, cardinality bounds, secret-canary and telemetry-redaction integration, health classification, and explain snapshots with semantic assertions.
- **Human-review triggers:** New raw field/label, health/readiness meaning, operator-visible error detail, sampling/export dependency, or instrumentation requiring semantic crate changes.
- **Parallelism:** Safe with other post-WP-120 packages if hooks already exist.
- **Recommended PR boundary:** Shared telemetry/health contracts; registry/subscribers; diagnostics; cross-path redaction evidence.

#### WP-185 — Server composition

- **Purpose:** Compose the final `riffdbd` process from shared-service gRPC, loopback MCP HTTP, outbox/projection workers, observability, lifecycle, and health without adding business semantics.
- **Hard dependencies:** WP-130, WP-140, WP-160, WP-170, and WP-180.
- **Upstream inputs:** Stable server registration/lifecycle ports, all adapters/workers, shared auth/service, component health, shutdown semantics, and safe telemetry.
- **Downstream interfaces:** Final component graph, bounded startup/shutdown, endpoint/worker ownership, authoritative-versus-derived health, and real projection public-path integration.
- **Principal risks:** Adding semantics in wiring, creating a second service instance, starting workers before durable recovery, incorrect shutdown ordering, HTTP audience mismatch, or health overstating derived readiness.
- **Acceptance evidence:** Server package and composition tests proving one process serves gRPC and loopback MCP HTTP, runs both workers, shuts down cleanly, reports health accurately, and satisfies real projection read-after-sequence through public APIs.
- **Human-review triggers:** Component ownership, new public listener, audience/binding change, lifecycle or health meaning, direct storage path, or any business rule in server wiring.
- **Parallelism:** None among its direct inputs; it begins after their stable integration ports merge.
- **Recommended PR boundary:** One narrow composition PR; semantic fixes go back to owning packages rather than expanding WP-185.

#### WP-190 — Integrated crash harness

- **Purpose:** Kill and restart real child processes at named authoritative boundaries and compare durable state with the reference model.
- **Hard dependencies:** WP-070, WP-100, WP-160, WP-170, and WP-185.
- **Upstream inputs:** Named failpoints already present, deterministic barriers/control protocol, temporary server lifecycle, durable inspector, and reference model.
- **Downstream interfaces:** Child-process controller, failpoint scenario DSL/config, state inspector, recovery report, and repeat-open harness.
- **Principal risks:** Replacing process death with recoverable errors, timing via sleeps, missing state dimensions, nondeterministic failpoint placement, or inspecting through a path that mutates recovery state.
- **Acceptance evidence:** Exact pre-commit or post-commit state at every SPEC failpoint across entities, indexes, outcomes, records, events, provenance, outbox, projection; repeated restart remains stable.
- **Human-review triggers:** Any third recovered state, mismatch with redb guarantees, sequence/event duplication, frontier skip, integrity repair ambiguity, or missing upstream hook.
- **Parallelism:** No useful production-package parallelism after WP-185; reporting preparation may overlap without editing the harness inputs.
- **Recommended PR boundary:** Failpoint/controller protocol; durable inspector/model; matrix cases; machine-readable report.

#### WP-200 — POC acceptance and release

- **Purpose:** Assemble the self-verifying budget demo, requirement report, reproducible benchmarks, threat/dependency evidence, and release artifacts used for the POC decision.
- **Hard dependencies:** WP-050, WP-075, WP-130, WP-135, WP-140, WP-150, WP-160, WP-170, WP-180, WP-185, and WP-190; these transitively cover the remaining packages.
- **Upstream inputs:** Stable binaries/protocols/formats, complete automated evidence, benchmark harnesses including the resolved Fjall experiment, known limitations, and security posture.
- **Downstream interfaces:** POC-001..010 JSON report, demo, benchmark report, SBOM/checksums, compatibility statement, release bundle, and architecture-review packet.
- **Principal risks:** Becoming an out-of-scope integration repair package, missing requirement ownership, irreproducible benchmarks, overstated durability/security claims, or release generation that changes tracked files.
- **Acceptance evidence:** Exact three YAML commands, clean `ci-all`, asserted demo through every transport, verified release artifacts/checksums/SBOM, and human sign-off for all ten POC criteria.
- **Human-review triggers:** Any implementation fix outside allowed release paths, omitted criterion, benchmark engine decision, security claim, format claim, or POC/MVP scope change.
- **Parallelism:** None; this is the final integration and evidence package.
- **Recommended PR boundary:** Acceptance/demo/report; benchmarks/threat/dependency documents; release packaging; then a separate human architecture decision, not an agent acceptance.

#### P2 / POC exit evidence

P2 proves that both MCP transports expose the same policy-filtered service, a stale or unauthorized tool cannot bypass policy, an uncertain outcome is recoverable, and a projection query after the winning commit cannot return a prefix missing that commit. POC exit additionally needs every POC criterion, recovery boundary, reproducible generator, dependency/security report, and the resolved storage comparison.

### 6.5 Roadmap Toward MVP

- **Stage A, single-node alpha:** Add a real migration framework, stable format policy, online consistent backup/verified restore, bundle signing, bounded indexed reads, approved repair operations, remote TLS/OAuth MCP, TypeScript/Python clients, quotas, projection/backfill controls, and upgrade/downgrade compatibility. Gate on a trusted design-partner workload with documented recovery and incident procedures.
- **Stage B, replicated beta:** Put deterministic normalized commit application behind a replication facade, add snapshots/membership/catch-up/leader routing, and prove idempotency/outcomes unchanged through leader loss and network partitions. Do not add Raft to the POC path.
- **Stage C, partitioned MVP:** Add tenant-local leaders, placement epochs, fenced movement, production authorization/audit/rate limits, stable clients, operational projections, agent branches/replay, CDC/export, and full operational support. Continue rejecting undeclared cross-partition mutations.
- **Post-MVP research:** Distributed transaction/saga semantics, escrow/commutative types, global uniqueness/indexes, multi-region placement, richer incremental analytics, and verified migrations remain explicit research tracks. They must not leak into POC abstractions beyond preserving deterministic, replication-shaped records.

The budget comparison application remains a compatibility and evidence canary across these stages. New guarantee profiles are added only when both backends implement the claimed observable semantics; historical workload fixtures remain runnable so API ergonomics, implementation complexity, correctness, and performance can be compared over time.

## 7. WP-000 Detailed Plan

WP-000 completed in commit `4407d59`. This section is retained as the reviewed
bootstrap design and acceptance record; its future-tense instructions are not a
request to recreate or replace the existing workspace.

### 7.1 Scope and completion prerequisites

The reconciled WP-000 scope includes `.gitignore`, every crate manifest, and
minimal `crates/*/src/**` targets, so no path exception is required. Do not use
its broad `scripts/**` or `adr/**` permission to preempt later package semantics.
A baseline commit, provisioned Rust 1.97.0/Cargo tools, confirmed CI provider,
and human-approved dependency license/source policy are required for final
acceptance.

### 7.2 Initial workspace skeleton

Create a virtual root workspace with `resolver = "3"`, an explicit (not glob-expanded) member list, and all 29 crates from SPEC Section 5:

```text
crates/
  riffdb-types/              riffdb-errors/
  riffdb-proto/              riffdb-contract-syntax/
  riffdb-contract-ir/        riffdb-contract-compiler/
  riffdb-catalog/            riffdb-storage-api/
  riffdb-storage-memory/     riffdb-storage-redb/
  riffdb-invariant/          riffdb-runtime/
  riffdb-conflict/           riffdb-idempotency/
  riffdb-commit/             riffdb-auth/
  riffdb-policy/             riffdb-service/
  riffdb-api-grpc/           riffdb-client-rust/
  riffdb-server/             riffdb-api-mcp/
  riffdb-mcp-stdio/          riffdb-cli/
  riffdb-outbox/             riffdb-projection/
  riffdb-observability/      riffdb-diagnostics/
  riffdb-testkit/
```

All packages are private and inherit workspace package metadata/lints. Twenty-six begin as library-only packages. `riffdb-server`, `riffdb-cli`, and `riffdb-mcp-stdio` should have minimal library roots for testable composition plus binary targets named `riffdbd`, `riffdb`, and `riffdb-mcp`. Their binary `main` functions remain empty bootstrap targets and must not claim server behavior.

Do not add `riffdb-codegen` or `riffdb-replay` packages. Their ownership is unresolved and the SPEC permits later helper/subcommand consolidation.

Each library root contains only crate-level documentation and `#![forbid(unsafe_code)]`. It must define no placeholder domain type, trait, module graph, fake service, or dependency. Each manifest has empty dependencies and no speculative features. This is non-semantic compilation scaffolding, not a public API.

### 7.3 Repository topology and ownership

The intended roots and their current manifest ownership are:

| Path | Initial/first owner | Policy |
|---|---|---|
| `adr/` | WP-000 and reviewed governance changes | Preserve the index/template and ADR history; only record acceptance after explicit human approval |
| `scripts/` | WP-000 plus exact package patterns | WP-020 owns `generate-proto*`; WP-040 `generate-contract-fixtures*`; WP-070 `storage_*`; WP-140 `mcp-inspector-smoke`; WP-150/WP-200 `demo*`; WP-190 `recovery_*`; WP-200 `release*` |
| `.github/` | WP-000; WP-045 owns only `workflows/budget-comparison.yml` | Core CI/PR templates stay with WP-000; the isolated comparison workflow follows its evidence package |
| `proto/`, `fixtures/proto/` | WP-020 | Real schema and compatibility artifacts only |
| `contracts/parser-fixtures/` | WP-030 | Selected grammar corpus |
| `fixtures/compiler/`, `contracts/examples/budget.riff` | WP-040 | Golden plans/schemas/diagnostics and canonical budget source |
| `fuzz/Cargo.toml`, `fuzz/fuzz_targets/contract_*` | WP-030 | Parser fuzz workspace/targets; later targets need a declared owner |
| `tests/contract_deploy*`, `tests/storage_recovery/**`, `tests/command_semantics/**` | WP-050, WP-070, WP-100 respectively | Catalog, storage recovery, and command semantic integration targets |
| `tests/authorization/**`, `tests/service/**`, `tests/grpc/**`, `tests/mcp/**` | WP-110, WP-120, WP-130, WP-140 respectively | Security, service, and protocol integration targets |
| `tests/outbox/**`, `tests/projection/**`, `tests/observability/**`, `tests/server_composition/**`, `tests/recovery/**` | WP-160, WP-170, WP-180, WP-185, WP-190 respectively | Derived-system, final-wiring, and recovery integration targets |
| `benchmarks/storage-fjall/**` | WP-075 | Isolated non-production Fjall workspace, adapter, suite, and evidence |
| `benchmarks/**` | WP-200 except the WP-075 subtree | Reproducible process scenarios/reports; component benches stay in owning crates |
| `examples/budget-comparison/**` | WP-045, WP-125, WP-135 | Isolated workload/PostgreSQL, service adapter, then public SDK/gRPC adapter under exact subpaths |
| `examples/**` | WP-150/WP-200 | Public API/demo packaging; preserve comparison-workstream ownership |
| `docs/` | WP-200 | Generated/reference docs and security/compatibility statements |

Do not add meaningless marker files merely to force empty directories into Git. `PLAN.md` records the topology until real artifacts exist.

### 7.4 Cargo and lint policy

- `rust-toolchain.toml`: exact channel `1.97.0`, `profile = "minimal"`, components `rustfmt` and `clippy`.
- Root workspace: Rust 2024 edition, explicit `rust-version = "1.97.0"`, `version = "0.1.0"`, `publish = false`, resolver 3, explicit members, and committed `Cargo.lock`. Do not invent authors, repository URLs, or a project license before those metadata decisions exist.
- Workspace Rust lints: `unsafe_code = "forbid"`, `unused_must_use = "deny"`, and warnings for missing docs and unreachable public items. Workspace Clippy enables its standard correctness group and denies holding a synchronous lock guard across await. Every package uses `[lints] workspace = true`, while every crate root independently contains `#![forbid(unsafe_code)]` so the policy is visible and cannot be weakened accidentally.
- Clippy: the acceptance command makes warnings fatal. Do not enable a large opinionated lint group that creates churn before code exists; add targeted lints when they enforce a documented invariant.
- Formatting: use the pinned rustfmt defaults unless a later approved `.rustfmt.toml` scope is added. Formatting output must be stable on 1.97.0.
- Features: empty default feature sets; features are additive; no feature may silently change correctness, durability, authorization, record format, or public protocol. Loom, Shuttle, failpoint, and memory-only server behavior are explicit test-only features owned later. Every all-features build must compile.
- Dependencies: WP-000 adds none. Future manifests disable broad default features when practical and document every critical dependency per `AGENTS.md`.
- `.gitignore`: initially ignore the root `/target/` only; later packages add narrowly justified generated runtime/fuzz/database output patterns without ignoring checked-in fixtures.

### 7.5 Dependency policy

`deny.toml` should establish:

- advisories and yanked crates denied, with time-bounded documented exceptions only;
- unknown registries and unknown Git sources denied;
- duplicate versions reported with an explicit policy rather than blindly banned where ecosystem constraints require them;
- wildcard dependency versions denied;
- a human-approved license allowlist and explicit handling for unlicensed/unknown packages;
- bans for dependencies that violate native-code, cryptography, or scope policy only after a reviewed reason is recorded.

`cargo-deny`, `cargo-audit`, and `cargo-machete` are CI tools, not workspace dependencies. Their exact versions or install-action revisions must be pinned in CI from WP-000 onward. `cargo deny check` is the exact WP-000 acceptance gate; audit and unused-dependency jobs are additional SPEC-required CI checks even while initially trivial.

### 7.6 CI jobs and cache boundaries

Use GitHub Actions with minimal read-only default permissions, explicit permissions per job, concurrency cancellation for superseded branch runs, and third-party actions pinned by full commit SHA.

1. **Format:** `cargo fmt --all -- --check`.
2. **Clippy:** `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
3. **Test:** `cargo test --workspace --all-features` (also run the exact WP command without feature expansion during acceptance).
4. **Docs:** `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
5. **Dependency policy:** pinned `cargo deny check`, `cargo audit`, and `cargo machete`. The latter two are initially trivial but establish the required supply-chain and unused-dependency jobs before dependencies arrive.
6. **Generated artifacts:** run the generic check and fail on any tracked or untracked diff.
7. **Workspace policy:** verify every first-party target has `#![forbid(unsafe_code)]`, inherited lints, no unexpected package, and exactly the three approved initial binaries.

Cache Cargo registry and Git data separately from compiled `target` output. Key compiled caches by OS/target, exact Rust toolchain, job/profile/features, and `Cargo.lock` hash. Never cache generated sources, compatibility fixtures, test databases, crash/recovery directories, fuzz corpora produced by CI, release artifacts, or secret-bearing configuration. Generation/recovery jobs must start from clean inputs.

Do not create empty fuzz, recovery, benchmark, or MCP conformance jobs. Add each only when its owning package contributes a real target and acceptance command.

### 7.7 ADR workflow and status convention

Validate and retain `adr/0000-template.md`, `adr/README.md`, and the ADR records
created by reviewed governance passes. The template distinguishes
direction approval from exact acceptance and includes the decision deadline,
context, decision, alternatives, consequences, compatibility/security/test
effects, affected requirements/work packages, and supersession links.

Allowed statuses are `Proposed`, `Accepted`, `Rejected`, and `Superseded`. New ADRs start `Proposed`. Only explicit human-maintainer approval authorizes an ADR to become `Accepted`; the implementation agent may perform the status edit after receiving that approval. Superseded records remain immutable except for status/link metadata.

### 7.8 Generation checks

Create a generic, deterministic `scripts/check-generated` convention. With `LC_ALL=C` and `TZ=UTC`, it finds executable regular files matching top-level `scripts/generate-*`, sorts paths bytewise, and invokes each with `--check`. It records a clean tracked/untracked baseline and fails if any generator exits nonzero or the worktree differs afterward. Zero discovered generators is valid at WP-000 because no generated source exists; this discovery convention lets later packages register a generator by adding only their allowed `generate-*` script.

Create `scripts/ci-all` for the standard commands, pinned audit/machete checks,
workspace policy, requirement-coverage checking, and generation verification.
Package-specific scripts such as `generate-proto`,
`generate-contract-fixtures`, and `mcp-inspector-smoke` are added only by their
declared owners. WP-000 must not create no-op placeholders.

### 7.9 WP-000 acceptance sequence

1. Run formatting and verify all targets use the pinned toolchain.
2. Run exact YAML Clippy and test commands.
3. Run all-features workspace tests and docs with warnings denied.
4. Run `cargo deny check`, `scripts/check-requirement-coverage`, and
   `scripts/ci-all`; the latter exercises pinned audit/machete, workspace policy,
   coverage, and `scripts/check-generated`.
5. Confirm the package/binary inventory and absence of production dependencies/public semantic items.
6. Repeat from a clean checkout of the baseline plus WP-000 changes and confirm `git status --porcelain` is empty after all checks.
7. Fill the exact `AGENTS.md` PR description, including the upstream revision,
   then stop.

## 8. Proposed ADR Queue

Accepted ADR-0002, ADR-0003, ADR-0005, ADR-0006, ADR-0010, ADR-0011, and
ADR-0013 through ADR-0016 are frozen inputs, not open choices. The smallest
remaining queue is ordered by the first work package it can block. Exact-text
acceptance always requires explicit human approval.

### ADR-0001 — Standalone Database Boundary

- **Why:** Record the fixed choice that the POC is its own server, not a PostgreSQL extension/control plane, and that replication remains later.
- **Blocks:** P0 under SPEC Section 22.1; WP-200 also lists it explicitly.
- **Options:** Standalone server; PostgreSQL extension; external semantic control plane.
- **Recommended proposal:** Accept the existing standalone single-node Rust direction; keep PostgreSQL/Fjall isolated as evidence and replication post-POC.
- **Decision point:** Include in the immediate P0 batch to satisfy the earlier of the conflicting deadlines without weakening either source.
- **Development mode:** Acceptance is governance-only; it does not require implementation work.

### ADR-0004 — Semantic Storage API and Redb Baseline

- **Why:** Storage/coordinator ownership and specialized catalog/capability/outbox/projection ports cannot be inferred safely from the illustrative trait.
- **Blocks:** WP-060, WP-070, and indirectly P0/P1.
- **Options:** Live GAT snapshot vs bounded materialized snapshot; engine-owned semantic `commit` vs coordinator-driven narrow typed transaction; specialized subtraits vs one broad trait; repair/backup boundaries.
- **Recommended proposal:** Bounded materialized snapshots; storage-api-owned intent/evidence records; coordinator-driven short typed transactions; specialized bounded ports; memory/redb conformance.
- **Decision point:** Must be accepted before WP-060 public traits merge.
- **Development mode:** Exact trait and durable-record shapes must be accepted before WP-060 implementation; redb mechanics may be developed later within that envelope.

### ADR-0009 — Opaque Server-Side POC Capabilities

- **Why:** WP-060 must persist a stable capability record before WP-110 can safely authenticate tokens.
- **Blocks:** WP-060, WP-070, WP-110, and later MCP authorization evidence.
- **Options:** Opaque random tokens vs self-contained tokens; exact HMAC frame/provider; current/previous key policy; audience, expiry, revocation, and audit ownership.
- **Recommended proposal:** Opaque OS-random 32-byte tokens returned once; persist only a versioned HMAC-SHA-256 digest and bounded authorization metadata; resolve and reauthorize on every request; coordinator-owned create/revoke audit.
- **Decision point:** Accept the bytes, DTO, provider, and lifecycle before WP-060; implementation may wait for WP-110.
- **Development mode:** Critical dependency selection needs human review and may not be inferred by a storage agent.

### ADR-0012 — Deterministic Transaction Context: Logical Time and POC Randomness

- **Why:** WP-080 conflicts with `TXN-011`, and `tx.time` must survive uncertain retries without hidden clocks.
- **Blocks:** WP-080 and P0.
- **Options:** Strict no command randomness; a future-only unexposed IR slot; admitted recorded randomness (which would require changing normative scope). For time: durable admission reservation, derivation from approved request identity, or another recorded source.
- **Recommended proposal:** No POC command randomness; coordinator records a UTC logical timestamp in pending admission before evaluation; retries reuse that time and exact plan; runtime receives only owned bounded values.
- **Decision point:** Accept before WP-080 and coordinate with the runtime-fault decision below.
- **Development mode:** Must be accepted before WP-080 implementation. No production experiment may expose randomness while `TXN-011` stands.

### ADR-0017 — Deterministic Runtime Fault Disposition (number/title proposed)

- **Why:** ADR-0013 fixes deterministic arithmetic faults as non-business failures with no `CommitIntent`, but does not fix what happens to the durable pending admission or what callers may safely do.
- **Blocks:** WP-080 and the recovery/idempotency paths in WP-100/WP-190.
- **Options:** Terminal sequenced execution-failure record; resumable pending failure; abandoned/cancelled admission; internal-defect vs a new stable public kind.
- **Recommended proposal:** To be finalized with ADR-0012 after reference-model review; no implementation default is authorized.
- **Decision point:** Accept before WP-080 implementation.
- **Development mode:** May be a narrow amendment to ADR-0012 instead of a separate ADR if that yields one coherent exact decision.

### ADR-0007 — Shared Application-Service Boundary

- **Why:** Service vs commit orchestration, transport authentication, repeated authorization, request/result DTOs, pagination, and derived-system ports must be fixed before adapters branch.
- **Blocks:** WP-100's external coordinator entry, WP-120, WP-130, and WP-140.
- **Options:** Fine-grained service traits vs one facade; transport-authenticate/service-authorize split; commit executor entry point; cursor/wait/result shapes.
- **Recommended proposal:** Transport authenticates into an internal principal; service authorizes every operation and applies obligations; commit owns command admission/execution; typed administration uses coordinator-owned audit; no adapter exposes storage.
- **Decision point:** Draft alongside WP-100; accept before WP-120 interface merge.
- **Development mode:** May be developed alongside WP-100 result/interface work; must be accepted before WP-120 implementation.

### ADR-0008 — Native MCP Tool and Resource Model

- **Why:** Tool normalization/collisions, resource URIs, schema reuse, visibility, stale-name denial, stdio-via-gRPC, HTTP composition, pagination, and text safety are public compatibility/security boundaries.
- **Blocks:** WP-140.
- **Options:** Qualified vs unqualified names; percent-encoded IDs vs stable numeric URI segments; stdio gRPC vs in-process; list-change/subscription behavior.
- **Recommended proposal:** `riffdb.cmd.<normalized_contract>.<normalized_command>` with deployment-time collision rejection; stable encoded resources; stdio uses public gRPC; configured HTTP audience; every invocation reauthorizes.
- **Decision point:** Develop after ADR-0007/service interface; accept before WP-140 fixtures.
- **Development mode:** May be completed after P1 service interfaces; must be accepted before WP-140 fixtures or MCP implementation.

## 9. Verification Matrix

| Guarantee | Techniques and required assertions | Earliest package | Final end-to-end evidence |
|---|---|---|---|
| Safe Rust, reproducible workspace, dependency policy | Crate-root `forbid`, fmt/Clippy/test/doc/deny, package inventory, clean generated diff; separately inventory transitive unsafe/native code | WP-000 | WP-200 `ci-all` and release verification |
| Canonical IDs/values/decimal/keys/hashes | Unit tests, Proptest ordering/overflow/scale algebra, cross-platform golden bytes/hashes, encoder/decoder fuzz | WP-010 | WP-100 identity/history tests and WP-200 demo |
| Public-safe errors and redaction | Unit/property bounds, safe conversion snapshots, secret canaries through logs/gRPC/MCP, business outcome not transport error | WP-010 | WP-180 telemetry redaction and WP-200 |
| Public/durable Protobuf compatibility | Golden old/current descriptors and records, reserved field tests, envelope/decoder fuzz, deterministic regeneration | WP-020 | WP-070/WP-190 recovery and WP-200 release |
| Parser bounds and diagnostics | Valid/invalid corpus, source-span snapshots plus semantic assertions, parser/round-trip fuzz with production limits | WP-030 | WP-040 fixture and WP-200 demo compile |
| Deterministic IR/plan/schema | Golden bundle/plan hash/JSON Schema, repeated-build equality, compatibility properties, unsupported IR rejection | WP-040 | WP-050 catalog, WP-140 tools, WP-200 |
| Runtime determinism and invariant preservation | Pure unit tests, property-generated command histories, same snapshot/input/time comparison, single-thread reference-model differential testing | WP-080 | WP-100 real concurrency and WP-200 POC-002/003 |
| Complete read/predicate tracking | Compiler negative tests, runtime evidence assertions, mutate each influential read before commit, safe rejection of undeclared dependencies | WP-040/WP-080 | WP-100 concurrency/revalidation and WP-190 |
| Snapshot/storage semantic conformance | One parameterized suite over memory and redb: absence/version, bounded scan/epoch, ordered scan, atomic state/model equality | WP-060 | WP-070 and WP-190 |
| Conflict exclusivity/fairness/cancellation | Unit tests; Loom reduced grant/release/cancel/timeout/fairness/lost-wakeup model; Shuttle multi-key schedules; barriers/hooks, no sleeps | WP-090 | WP-100 budget race and WP-200 |
| Atomic sequence/mutations/outcome/events/provenance/log | Reference properties and failpoints before/during/after durable transaction; inspect the entire record set and sequence visibility | WP-060 memory, WP-070 durable | WP-190 matrix and WP-200 |
| Idempotent uncertain-response recovery | Canonical identity/hash properties, equal/mismatch cases, kill after durable commit, same sequence/result and no second mutation/event | WP-100 | WP-190 and WP-200 POC-004/005 |
| Catalog activation/compatibility | Expected-version races, compatibility golden fixtures, process crash before/after pointer, notification timing | WP-050 | WP-140 list changes, WP-190/WP-200 |
| Authorization non-bypass and obligations | Deny-default matrix, revocation/environment/audience/tenant/field tests, architecture dependency check, stale MCP name invocation, secret canaries | WP-110 | WP-120 service, WP-130 gRPC, WP-140 MCP, WP-200 POC-008 |
| API-neutral service | In-process fake/real port integration, dependency graph checks forbidding concrete storage imports, policy decision assertion per operation | WP-120 | WP-130/WP-140 transport E2E and WP-200 |
| gRPC conformance and client retry | Descriptor fixtures, status-vs-outcome mapping, malformed/limit/deadline tests, generated signature snapshots, same-key safe retry/unknown outcome | WP-020/WP-130 | WP-200 POC-001/004 |
| MCP schema/protocol/authorization | Golden tools/resources, JSON/canonical conversion fuzz, output-schema validation, init/list/page/progress/cancel tests, Inspector both transports, per-tool auth | WP-140 | WP-200 POC-001/007/008 |
| Outbox atomic intent and at-least-once | State-machine properties, fake connector, commit-log comparison, crash before dispatch/after destination success, duplicate visible | WP-160 | WP-190 and WP-200 POC-005 |
| Projection prefix/frontier/rebuild | Reference prefix, monotonic property, `(projection,sequence)` idempotency, typed wait states, apply/frontier/wakeup crash tests | WP-170 | WP-190 and WP-200 POC-006 |
| Recovery equivalence | Named process failpoints, child kill/reopen, durable inspector/reference comparison, repeated-open idempotence | WP-050/WP-070 | WP-190 full matrix and WP-200 POC-009 |
| PostgreSQL/RiffDB example parity | Golden workload/observation fixtures and PostgreSQL tests in WP-045; parameterized service-adapter conformance in WP-125; public SDK/gRPC conformance in WP-135; correctness preflight before every benchmark | WP-045 | WP-200 process comparison report using the same runner |
| Performance and regressions | Criterion for value/lock/storage/runtime; process workloads for conflict-free/hot-key/replay/scan/restart/durability; record revision/environment/distribution | WP-070, then WP-090/100/170 | WP-200 published redb/Fjall report |
| Generated-artifact reproducibility | WP-000 registry; byte-for-byte proto, IR/schema/docs, SDK, MCP regeneration; clean tracked/untracked status | WP-000 | WP-200 release verification |

Fuzz, Loom, Shuttle, and full crash jobs run in dedicated profiles. Process correctness tests use explicit control barriers, never sleeps. Benchmarks are architecture evidence and regression signals, not an invented TPS release threshold.

## 10. Risk Register

| Risk | Trigger / early warning | Affected WPs | Consequence | Mitigation and required decision/experiment | Threat |
|---|---|---|---|---|---|
| Contract language scope growth | Requests for loops, callbacks, arbitrary scans/I/O, both conflicting syntaxes, or runtime AST access | WP-030/040/080 | Static read/effect/conflict visibility and termination no longer hold | Select one grammar; reject unsupported constructs; require semantic corpus/model evidence and human review for every construct | Core concept if static visibility is lost |
| Hidden nondeterminism | Runtime imports clocks/random/env/filesystem, unordered map serialization, variable bundle metadata, or schedule-dependent output | WP-010/040/080/100 | Replay/model/replication-shaped semantics diverge | ADR-0011/0012; pure value context; canonical collections; determinism fixtures and differential histories | Core concept |
| Storage transaction held across execution | Live redb transaction or guard appears in runtime context/intent; long writer stalls | WP-060/070/080/100 | Throughput collapse, cancellation hazards, engine leakage, unclear atomicity | ADR-0004 prototype comparing bounded snapshot plus coordinator transaction; compile-time ownership tests | Selected implementation, unless needed for correctness |
| Incomplete read dependency tracking | Runtime reads absent from plan/evidence; invariant fails only under interleaving | WP-040/060/080/100 | Invalid committed state | Compiler fail-closed analysis, materialized evidence for every read, mutate-before-commit adversarial tests, reference model | Core concept |
| Predicate revalidation cannot use exact plan | Only invariant ID/captured values survive, catalog lacks historical plan, plan hash mismatch | WP-040/050/060/080/100 | Commit validates the wrong rule or cannot validate | ADR-0003 chooses embedded validated commit-check representation or historical immutable lookup; version/hash assertions | Core concept |
| Lock cancellation/fairness defects | Lost wakeup, queue growth, double grant, starved multi-key waiter, leaked lease after cancellation/panic | WP-090/100 | Deadlock, availability failure, or conflicting execution | Reduced Loom model, Shuttle schedules, RAII lease, explicit barriers, bounded queues and wait metrics | Selected implementation; double grant threatens concept proof |
| Idempotency fails after uncertain response | Duplicate event/mutation, different result/sequence, lost `tx.time`, orphan reservation | WP-060/070/100/130/140/190 | POC’s uncertainty-recovery thesis fails | ADR-0005; atomic terminal set; reservation crash model; kill after commit and same-key retry | Core concept |
| Durable format evolves unsafely | Field/key reuse, fixture drift, decode/re-encode loses required data, table examples disagree | WP-020/050/060/070/100/170 | Startup failure, silent reinterpretation, unrecoverable data | ADR-0006; reserved fields, versioned envelopes/keys, old/current fixtures, offline idempotent migration policy | Implementation now; product viability if unmanaged |
| MCP authorization bypass | Adapter imports storage, visibility substitutes for auth, stale hidden tool succeeds, obligations applied after rendering | WP-110/120/140/180/200 | Unauthorized data access/mutation and failed thesis | Dependency architecture test, service-only adapter, invoke-time reauthorization, stale-name and secret-canary tests | Core concept/security |
| Projection frontier is incorrect | Frontier exceeds applied state, decreases, skips commit, or after-sequence returns stale data | WP-060/070/170/190/200 | Derived consistency claim is false | ADR-0010; atomic state/frontier, idempotent per-sequence apply, prefix model and crash tests | Core POC proof; engine is replaceable |
| Excessive crate fragmentation | One-line forwarding crates, cyclic dependencies, duplicate DTOs, frequent cross-crate changes | WP-000 onward | Slow integration and accidental public interfaces | Keep 29 specified crates but no fake APIs; owner map; package-private types where possible; combine PRs by semantic boundary, not new crates | Selected implementation |
| Agents change shared types concurrently | Parallel PRs edit `riffdb-types`, IR, storage API, proto, service DTOs, or fixtures | All waves | Incompatible assumptions, duplicated types, unstable durable/public contracts | Interface-first PRs, named owner, merge revision in prompts, coordinated rebase, stop on out-of-scope interface need | Delivery risk |
| Proto freezes before semantics | Later package needs a field/service not represented and lacks proto scope | WP-020/050/100/110/130/160/170 | Out-of-scope changes or accidental compatibility break | Reviewed record/service inventory, phased schema ADR, reserve only justified fields, conversion boundaries | Selected implementation |
| Control-plane write bypass | Catalog/capability code mutates redb independently of coordinator with unclear sequence/audit | WP-050/070/110/120 | Violates sole-mutation boundary or creates untracked authority changes | Classify control-plane state; if authoritative, route it through the commit coordinator. Any exception requires a human amendment to the boundary; never allow direct API storage. | Core architecture boundary |
| Final composition drifts across component ports | WP-185 needs semantic changes or constructs a second service/policy path | WP-130/140/160/170/180/185 | Individually correct components fail or bypass policy in the real process | Freeze registration/lifecycle ports upstream; keep WP-185 wiring-only; return semantic fixes to owners; run public-path server composition tests | Selected work-package design |
| Comparison interface erases RiffDB semantics | Shared code begins exposing storage CRUD or only the guarantees PostgreSQL provides cheaply | WP-045/125/135/200 | Example becomes misleading and pressures core APIs toward a least-common denominator | Share workload/oracle and normalized observations only; keep backend adapters separate and forbid example dependencies from RiffDB crates | Evidence design risk; core concept if it shapes production APIs |
| Comparison benchmark is not equivalent | Backends use different durability, isolation, transports, guarantee profiles, warmup, pooling, or datasets | WP-045/125/135/150/200 | Misleading performance or complexity claims | Correctness preflight, explicit guarantee profiles, matched workload controls, separate component/process results, complete environment metadata | Evidence risk only |
| PostgreSQL comparison leaks into the critical path | A `riffdb-*` crate imports the driver/SQL or CI makes PostgreSQL required for core semantic tests | WP-045/125/135 and all production WPs | Violates architecture scope and couples RiffDB to an external database | Isolated private example package, dependency architecture check, separate pinned service-container CI tier | Selected implementation boundary |
| Observability leaks sensitive values | Raw keys/claims/free text appear in spans, metrics, MCP text, errors, or snapshots | WP-010/110/120/140/180 | Security failure and misleading model-facing content | Redact before subscriber, bounded labels, static descriptions, safe rendering, canary tests | Implementation/security |
| Recovery harness gives false confidence | Tests use recoverable errors/sleeps, omit indexes/outcomes/events/provenance/frontier, or inspect after repair | WP-070/100/160/170/190 | Torn states survive despite green tests | Named barriers, real process kill, full durable inspector, reference prefix comparison, repeated reopen | Evidence risk; concept if failures appear |
| Redb-specific bottleneck mistaken for concept failure | Single-writer/storage flush dominates but semantic model costs are not isolated | WP-070/100/200 | Wrong go/no-go decision | Component/process benchmarks, same semantic suite on approved Fjall adapter, attribute bottleneck | Selected implementation only |

## 11. Decisions Requiring Human Input

The immediate human-review batch is deliberately larger than the next package so
WP-040, WP-060, WP-080, and WP-090 can proceed with fewer interruptions:

1. **Before P0/WP-060:** accept exact ADR-0001, ADR-0004, and ADR-0009 text after
   resolving the storage/capability audits. This freezes the standalone boundary,
   storage semantic transaction, persistence ports, capability record, token
   framing/provider, and create/revoke audit ownership.
2. **Before WP-080:** accept exact ADR-0012 plus the deterministic runtime-fault
   disposition required by ADR-0013. No implementation may infer whether a fault
   is terminal, sequenced, retryable, abandonable, or exposed as a particular
   public error.
3. **During the first WP-040 interface PR:** review the generated canonical
   `FORMAT.md` and `JSON_SCHEMA_FORMAT.md` registries before their compatibility
   fixtures merge. The generator may produce the review artifact, but only the
   maintainer can approve it as the concrete realization of ADR-0013.
4. **Before WP-100/WP-120:** accept ADR-0007's API-neutral service/coordinator
   boundary. This can be drafted during P0 but need not block WP-040/WP-060.
5. **Before WP-140:** accept ADR-0008's MCP name, resource URI, audience,
   discovery, and invocation-time authorization boundary.
6. **At WP-045 and WP-075 dependency review:** approve the PostgreSQL and Fjall
   versions/drivers and their license, unsafe/native, and reproducibility posture.
   This must not add either dependency to the root workspace.
7. **Post-POC only:** decide whether code generation becomes a `riffdb contract
   generate` subcommand and whether privileged replay/repair warrants a separate
   binary. WP-000 and the POC do not create `riffdb-codegen` or `riffdb-replay`.

SPEC Section 22.2 retains six defaulted decisions. Implementations use the stated
default until human review resolves the decision by its deadline:

1. **Read-only outcome journal, before WP-100 exit:** journal only when
   idempotency or audit requires it; create no mutation-log record otherwise.
2. **Multi-domain reads, before WP-080 exit:** allow them only when every mutable
   domain is declared up front and every influential read is version-tracked.
3. **Index phantoms, before write-influencing indexed range reads enter the POC:**
   keep initial indexed ranges read-only; otherwise defer them or require explicit
   conflict keys.
4. **Destructive contract changes, before WP-050 exit:** support additive and
   documentation-only changes; reject field or command removal.
5. **Remote TLS provider, before remote MCP alpha:** defer this POC-external
   dependency/platform ADR until the alpha boundary.
6. **Redb versus Fjall for MVP, at POC exit:** keep redb unless the unchanged
   workload, conformance, recovery, and benchmark evidence justifies a change.

The initial budget create/seed decision is no longer open; accepted ADR-0015 owns
it. No other unresolved architecture-direction choice is known before P0, P1,
or P2 beyond the immediate batch, these defaults, and later ADR acceptance at
the recorded deadlines.

## 12. First Execution Handoff

This handoff is archived: it was executed as WP-000 commit `4407d59` against the
`3235477` baseline. It remains below to preserve the original planning deliverable
and must not be rerun against the current implementation branch.

```text
You are implementing WP-000 only in /home/user/dev/riffdb.

Before claiming completion:
1. A maintainer has created a baseline Git revision containing the authoritative inputs so the PR can name its upstream revision and a fresh checkout can be tested.
2. Rust 1.97.0 with rustfmt/Clippy and pinned compatible cargo-deny, cargo-audit, and cargo-machete are available in the acceptance environment.

Authoritative inputs:
- AGENTS.md
- SPEC.md version 0.2
- work_packages.yaml, especially WP-000
- Every adr/*.md whose status is Accepted at the baseline revision (none exist at this governance handoff)

Required planning context (not additional authority):
- diagrams/poc_architecture.dot
- diagrams/command_sequence.dot
- diagrams/work_package_dag.dot
- diagrams/roadmap.dot
- PLAN.md
- Proposed ADR-0001 through ADR-0012; they record approved planning direction but are not authoritative and must not be marked Accepted

Requirement IDs:
- SYS-001
- SYS-002
- SYS-003
- REP-001
- POC-010

Upstream revision:
- Record the exact authoritative baseline commit in the PR description. WP-000 has no package dependencies.

Current allowed paths from work_packages.yaml:
- .gitignore
- Cargo.toml
- Cargo.lock
- rust-toolchain.toml
- deny.toml
- .github/**
- scripts/**
- adr/**
- crates/*/Cargo.toml
- crates/*/src/**

Required deliverables:
- A virtual Cargo workspace containing all 29 crates listed in SPEC Section 5.1.
- Minimal non-semantic library roots; no public domain types, traits, fake modules, or placeholder services.
- Thin bootstrap binary targets: riffdbd from riffdb-server, riffdb from riffdb-cli, and riffdb-mcp from riffdb-mcp-stdio.
- Rust 1.97.0, edition 2024, resolver 3, explicit rust-version, private packages, workspace lint inheritance, and committed Cargo.lock.
- `#![forbid(unsafe_code)]` in every first-party crate root.
- Empty default/additive feature policy and no Cargo dependencies added for future convenience.
- deny.toml and GitHub CI for formatting, Clippy, tests, docs, dependency policy, workspace policy, and generated-artifact cleanliness.
- ADR template/index using Proposed/Accepted/Rejected/Superseded; do not mark any ADR Accepted.
- Generic deterministic scripts/check-generated and scripts/ci-all. Do not create no-op package-specific generators.
- Machine-checked normative-requirement coverage for work_packages.yaml.
- Required AGENTS.md PR-description fields, including exact allowed paths, upstream revision, generated artifacts, limitations, and follow-ups.

Acceptance commands from WP-000:
- cargo fmt --all -- --check
- cargo clippy --workspace --all-targets --all-features -- -D warnings
- cargo test --workspace
- cargo deny check
- ./scripts/check-requirement-coverage

Additional required verification from AGENTS.md/SPEC:
- cargo test --workspace --all-features
- RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
- ./scripts/ci-all
- ./scripts/check-generated
- Run the checks from a fresh checkout and verify the worktree remains clean.

Explicit non-goals:
- Do not implement Protobuf, contract grammar, IR, compiler, catalog, storage, runtime, conflict management, idempotency, commit, authorization, policy, services, gRPC behavior, MCP behavior, CLI behavior, outbox, projection, observability, or recovery.
- Do not introduce semantic interfaces merely to make crates look complete.
- Do not add production dependencies, unsafe/native code, cryptography, SQL, Raft, distributed transactions, arbitrary callbacks, or analytics.
- Do not create riffdb-codegen or riffdb-replay crates.
- Do not create the PostgreSQL/RiffDB comparison application in WP-000; WP-045 begins it after WP-040.
- Do not modify AGENTS.md, SPEC.md, work_packages.yaml, PLAN.md, diagrams, or an Accepted ADR. Do not change a Proposed ADR's status or semantic direction.
- Do not start WP-010.

Stop and request human review if:
- The baseline is absent.
- Any deliverable requires a file outside allowed paths.
- A semantic/public/durable interface appears necessary.
- A dependency, unsafe/native code, cryptography, or critical tool cannot be pinned and reviewed.
- The license/source policy needs a maintainer decision.
- Any authoritative conflict is discovered.

Run and report every acceptance command, exact files changed, generated-artifact status, known limitations, and follow-ups. Stop after WP-000; do not proceed automatically into WP-010.
```
