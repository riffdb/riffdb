# RiffDB Implementation Plan

## 1. Repository Assessment

### Current contents

The repository is now bootstrapped and contains the 29-crate Rust workspace,
quality and generation scripts, GitHub Actions configuration, ADR records,
Protobuf sources and compatibility fixtures, the bounded contract parser and its
corpus/fuzz target, and the original planning inputs and diagrams. WP-000 through
WP-030 have landed, including the earlier focused WP-010 key/plan-identity and
WP-030 explicit-binding follow-ups. This governance batch creates one final
focused WP-010 completion follow-up for the accepted nonzero IDs, authorization/
audit vocabulary, capability/projection types, and exact execution-failure error
slice. WP-040 must not start until that follow-up passes the amended WP-010
acceptance commands and merges.

ADR-0001 through ADR-0007 and ADR-0009 through ADR-0021 are Accepted. ADR-0008
remains Proposed and is the only architecture ADR still required on the POC
critical path. ADR-0020 has already removed command tool-name grammar,
normalization, collision handling, and compiler/catalog ownership from its open
scope. The remaining URI, audience, transport, cursor-presentation, and MCP
presentation text must be accepted before WP-180 or WP-140 starts, whichever is
earlier, because both packages declare ADR-0008 as a required decision.

### Git state

Git is initialized on `main`; WP-000 landed as `4407d59`, and the implementation
baseline immediately preceding the accepted storage/service/projection governance
batch is `c07bc42`. No remote is configured yet. The maintainer has confirmed that
the repository will be pushed to GitHub, so GitHub Actions remains the CI provider
and adding the remote can wait until publication.

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
canonical partition/index keys. It also freezes pure UUIDv7 construction and
source ownership, initialization identity, provenance replay, exact audit
admission/terminal behavior, and separate authoritative-versus-derived recovery.
ADR-0019 freezes the exact six-category POC metadata surface and unconditional
startup validation without deferred operational placeholders. ADR-0020 freezes
the early compiler-owned MCP command-name registry and catalog revalidation.
ADR-0021 freezes the exact lineage-scoped service-audit targets, canonical
ordering, and shared-service-only construction rule. Section 2 lists the
remaining package-level freezes.

The 2026-07-14 accepted clarification additionally freezes the command write-
transaction order and exact durable-event hash preimage. A candidate begins with
a count check only, validates current state before deriving and reading mutation-
affected epochs, reserves an exact sequence-free plan against semantic and
conservative encoded capacity, and only then receives a sequence. `EventHash`
uses the exact EventId/EventTypeId/u32-length-framed canonical record preimage
under `riffdb.event/v1`.

### Missing prerequisites for WP-000

None. WP-000 is complete. Its baseline, toolchain, dependency policy, CI provider,
workspace inventory, generated-artifact convention, and acceptance evidence were
frozen before semantic implementation began.

## 2. Consistency Findings

The reconciled manifest has 53 unique work-package IDs and an acyclic dependency
graph. Gate membership, SPEC package table, YAML dependencies, and the work-package
DAG agree. All referenced normative requirement IDs exist or are explicitly
classified by the manifest's coverage policy. The 29 crate names and three POC
binaries (`riffdbd`, `riffdb`, and `riffdb-mcp`) agree across the specification
and package scopes. Allowed paths now cover each declared deliverable and
acceptance script. Public/durable ownership and MCP/gRPC/CLI/SDK layering agree
with the shared-service and coordinator boundaries.
The manifest identifies SPEC version 0.39. Its P3 application-platform sequence
extends the completed kernel proof with symbolic RiffQL, composite one-snapshot
reads, immutable query modules, and the ADR-0055 safe-application boundary. The
proposed P4 sequence adds the Agent Application Alpha gate before operational
or distributed alpha work. The earlier packages assign the accepted pre-sequence
reservation, envelope-bound, event-hash, single-terminal-outcome-row, and POC
state-machine-deferral evidence to the existing
WP-060/WP-065/WP-070/WP-100/WP-190/WP-200 owners without changing their IDs,
dependencies, or acceptance commands. The separately approved WP-060
`riffdb-types/src/codec.rs` exception is the only allowed-path addition.

### Blocking before WP-000

None. All identified WP-000 blockers were resolved before commit `4407d59`.

### Blocking before P0

None. The foundational key/binding/UUID follow-ups and the exact ADR-0001,
ADR-0004, ADR-0007, ADR-0009, ADR-0012, ADR-0017, ADR-0018, ADR-0019, and
ADR-0020/ADR-0021 decisions are frozen. WP-040 still has
its required human review of the generated `FORMAT.md` and
`JSON_SCHEMA_FORMAT.md`, but that is an interface-PR review trigger rather than a
pre-package architecture blocker.
The accepted ADR-0004/ADR-0011 clarifications remove the earlier ambiguity about
post-current-read aggregate capacity and event-hash content; neither remains a
P0 decision.

### Blocking before P1

ADR-0006 leaves the exact binary carriage of `PublicError` on non-OK gRPC
responses for human review before WP-130 conformance freezes. This does not block
earlier P1 packages. Formal WP-065 and WP-127 own the durable and public schema-
completion boundaries, respectively, and are explicit P1 gate members.

### Blocking before P2

No additional authoritative-input contradiction remains. ADR-0008 must be
accepted before WP-180 or WP-140 starts, whichever is earlier; it freezes the
remaining MCP resource URI, audience, transport, cursor-presentation, and safe
presentation/observability boundary. Command tool names are no longer open:
ADR-0020 assigns them to WP-040 compilation and WP-050 activation validation.
WP-130 owns the minimum runnable production server composition and P1 restart
proof; WP-185 extends that exact graph with P2 components. WP-075 owns the
isolated Fjall comparison, and WP-140 owns its Inspector script.

### Non-blocking clarifications

- `riffdb-codegen` and `riffdb-replay` remain deliberately deferred. WP-000 creates
  no such crates; code generation may become a `riffdb contract generate`
  subcommand, while privileged replay/repair is reconsidered after the POC.
- WP-020 owns the phase-zero Protobuf policy, common values/envelopes, and service
  descriptors. WP-065 owns later durable semantic messages, while WP-127 owns
  later public request/result messages; neither reopens WP-020.
- ADR-0019 deliberately creates no `NodeId`, clean-shutdown marker, persisted
  integrity-history value, reserved field, key, source port, or placeholder.
  Their canonical POC absence is valid, every startup runs the complete read-only
  authoritative validation, and later addition requires a separate migration ADR.
- ADR-0020 makes the versioned command-name registry part of the immutable
  compiler bundle. WP-050 validates it without repair, the service carries it
  verbatim, and WP-140 consumes rather than derives command tool names.
- ADR-0021 makes every contract semantic audit target lineage-scoped, freezes
  the nine semantic tags and canonical zero-through-16 collection, and assigns
  request-only target derivation to WP-120. Storage and transports only preserve
  the checked list; result links and returned/traversed objects are not targets.
- Startup validation is deliberately split without a dependency cycle. WP-060
  owns structural-session/evidence/type-state DTOs, WP-070 produces exact-end
  structural evidence and dormant ports, WP-050 alone interprets historical IR
  and produces opaque same-session `ValidatedCatalogHistory`, and WP-130 is the
  first layer that may combine them into operational readiness. WP-185 only
  preserves and extends that accepted P1 graph.
- The isolated WP-045 PostgreSQL baseline is accepted as `postgres =0.19.14`
  with default features disabled, `serde_json =1.0.150` with only `std`, and CI
  image `postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818`.
  It uses synchronous `NoTls` directly, with no ORM, pool, TLS integration,
  native dependency, or root-workspace production edge. Fjall dependency and
  version choices remain deferred to WP-075 review in a separate nested
  workspace.
- The exact reviewed capability baseline is `base64` 0.22.1 (`alloc`),
  `getrandom` 0.3.4 (no optional features), and `zeroize` 1.8.1 (`alloc`). Any
  version, feature, or transitive-graph change requires a new dependency review.
- Because the root is a virtual workspace, every top-level process/integration
  test is registered as one explicit `[[test]]` target in its owning crate
  manifest and invoked with a package-qualified command. WP-060/WP-080 may edit
  the testkit manifest/root only to register their declared model/history modules;
  no generic root test-harness crate is introduced.
- Root-workspace packages may update the generated root `Cargo.lock` within their
  declared paths. That path permission records reproducibility mechanics and does
  not approve an external dependency or feature graph.
- Influential range-epoch dependencies and mutation-affected epoch advances are
  distinct sets. The former prove evaluation reads; the latter are derived from
  transaction-current old and proposed new index entries after private
  validation. Exact-prefix overlap is allowed but equality is not assumed.
- All knowable input/runtime/component bounds remain pre-transaction. Only the
  exact aggregate staged-write reservation waits for bounded transaction-current
  values; it occurs before sequence assignment, and each actual canonical
  `StoredEnvelope` must fit its retained WP-065 per-class upper bound before
  staging.

## 3. Planning Assumptions

1. `AGENTS.md`, `SPEC.md`, `work_packages.yaml`, and Accepted ADRs are
   authoritative. Proposed ADRs and diagrams are planning context only.
2. The reconciled YAML hard dependencies are preserved exactly, and allowed
   paths are authoritative as amended by explicit maintainer approval. Soft
   sequencing below never removes a declared dependency.
3. All first-party crates are private Rust 2024 workspace packages on the fixed
   Rust 1.97.0 baseline. Linux CI is gating; macOS is best effort and Windows is
   outside the POC gate.
4. Redb 4.1.0 with default features disabled and no optional features is the
   reviewed production POC backend behind the semantic storage API. It is owned
   only by `riffdb-storage-redb`; no redb type crosses into runtime, service,
   policy, or transport crates. Dependency approval does not substitute for
   WP-070 durability, crash, and conformance evidence.
5. POC commands have no randomness. Logical time is fixed in durable admission
   and passed as an immutable transaction input. UUIDv7 generation remains
   outside runtime and storage: foundation types only assemble checked values,
   and the exact auth/client/server system-source owners are fixed by ADR-0018.
   The POC has a permanent durable `DatabaseId` but no `NodeId` or process-
   incarnation identity; ADR-0019 adds no substitute identifier source.
6. Public service DTOs are transport-neutral. Prost, Tonic, rmcp, redb, and
   transport status types remain inside adapters.
7. JSON Schemas and ADR-0020's versioned command tool-name registry are compiler
   artifacts in deterministic contract bundles. The catalog revalidates names
   on activation, the service carries them verbatim, and MCP wraps/consumes them
   without creating a competing schema or naming implementation.
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
12. `CommitSequence` and `AdministrationSequence`, never UUID ordering or wall
    time, establish authoritative order. Clock failure is fail-closed; it is not
    repaired by a UUID timestamp or a different clock domain.
13. `EventHash` is SHA-256 under the existing `riffdb.event/v1` frame over exact
    12-byte `EventId`, big-endian `EventTypeId`, big-endian `u32` payload length,
    and complete canonical `Value::Record` bytes. Payload-only, JSON, or
    Protobuf-derived event hashes are invalid.

## 4. Architectural Interface Map

The following map is the minimum interface freeze needed before agents branch.
Ownership and direction were accepted through the 2026-07-14 governance batch
and are reflected in the reconciled specification and manifest. ADR-0019 and
ADR-0020 are frozen inputs. The remaining Proposed ADR-0008 affects only MCP
resource URI, audience, transport, cursor-presentation, and presentation-text
compatibility; those parts cannot freeze until its exact text is Accepted.

| Interface | Owning crate / first producer | First consumers | Required stability and ADR | Freezing evidence |
|---|---|---|---|---|
| Stable IDs, assigned sequences/versions, canonical values, fixed-scale decimal/money, entity keys, logical timestamps | `riffdb-types`, WP-010 | WP-020, WP-030, WP-040, WP-060, WP-090, then all packages | Nonzero construction/decoding, byte encoding, bounds, checked arithmetic, ordering, and cross-crate ID inventory stable before parallel work; ADR-0005/0009/0011/0012/0013/0017/0018 | Zero-rejection and golden byte/hash vectors, decimal overflow/scale properties, ordering and round-trip tests, encoder fuzz |
| UUIDv7 values and production sources | Six newtypes/pure assembler in `riffdb-types`, WP-010; auth source WP-110; client source and one private server system-source primitive plus request/incident/database/provenance wrappers and production composition WP-130 | Initialization executor gets checked DatabaseId; WP-100 gets injected ProvenanceId; clients/transports/diagnostics get values or narrow ports; WP-185 reuses the same providers | Exact assembly, no UUID ordering, no ambient source in types/runtime/storage, auth/client/server-only `getrandom`; one server primitive prevents duplicate logic; fresh RequestId; bootstrap retains ID/token while normal create may return token-unavailable; ADR-0018 | Exact goldens/rejection, dependency-owner checks, primitive/wrapper fake-source calls, database/provenance collisions, bootstrap and normal-create lost-response tests |
| Public-safe errors and internal incident layering | `riffdb-errors`, WP-010, including consumer-owned `IncidentIdSource`; exact ADR-0012 execution-failure wire slice in `riffdb-proto`, WP-010 | All semantic crates, then adapters; server supplies incident source | Stable safe code/class/retry disposition and maximum detail; internal sources remain private; no concrete clock/entropy or fallback incident ID in errors. The narrow error-schema carve-out freezes only reviewed symbols; WP-127 owns remaining public fields; ADR-0018 | Secret-canary/redaction properties, bounded snapshots, source failure/no-fallback tests, execution-failure wire goldens, business-outcome-not-error assertions |
| Syntax AST, source map, parser diagnostics | `riffdb-contract-syntax`, WP-030 | WP-040 and diagnostics only | Grammar version and canonical budget source fixed; AST is neither durable nor runtime input; ADR-0002 | Valid/invalid corpus, source-span snapshots plus semantic assertions, bounded parser fuzz |
| Typed HIR, executable IR, command/read/invariant/projection plans, transport-neutral JSON Schemas, command-name registry | `riffdb-contract-ir`, WP-040; compiler produces instances | WP-060 consumes only immutable projection-schema values; WP-050, WP-080, WP-120, WP-130 codegen, WP-140, WP-170, WP-180 consume their owned views | Stable IDs, instruction validation, IR version, projection-group schema, canonical plan-hash inputs, and versioned command-name registry before WP-050/WP-060/WP-080. POC grammar/IR v1 contains no contract state-machine placeholder, transition instruction, or execution surface; ADR-0002/0017/0020 | Golden budget IR/hash/schema/explain, projection-schema bounds, exact name grammar/length/collision/source-span fixtures, repeated-build equality, unsupported-version and state-machine-surface rejection |
| Immutable `ContractBundle` | `riffdb-contract-ir`, WP-040 | `riffdb-catalog`, runtime/service/MCP by reference | Separate application version, compiler version, IR version, source hash, and plan hash; no `generated_at` in canonical content; canonical encoding/hashes cover the CommandId-ordered ADR-0020 name registry; ADR-0002/0006/0013/0020 | Canonical bundle fixture, name-registry/hash fixture, compatibility fixture, deterministic regeneration |
| Compiled MCP command tool-name identity | Checked registry value in `riffdb-contract-ir` and exact derivation/diagnostics in `riffdb-contract-compiler`, WP-040; `riffdb-catalog`, WP-050 revalidates | WP-120 carries policy-filtered descriptors; WP-140 consumes exact names; WP-185 composes dispatch | Exact `riffdb.cmd.<contract>.<command>` v1 lowercase mapping, `[a-z][a-z0-9_]*` segments, 128-byte total bound, CommandId order, full-name uniqueness, lineage/ID binding, and rename compatibility are frozen before WP-040 completes. No layer repairs, suffixes, truncates, reorders, or rederives; ADR-0020 | WP-040 grammar/boundary/case-collision/order/hash/compatibility goldens and properties; WP-050 missing/duplicate/reordered/misbound/noncanonical activation rejection; WP-120/WP-140 verbatim-consumption architecture tests |
| Active catalog snapshot, `ValidatedCatalogHistory`, deployment CAS, catalog notifications | `riffdb-catalog`, WP-050 | WP-100/WP-120 consume catalog state; WP-130 consumes `ValidatedCatalogHistory`; WP-140 consumes descriptors | Active pointer changes only after durable bundle through a typed coordinator administrative operation. Historical resolution consumes every ordered `HistoricalSemanticEvidence` page through exact end and alone creates opaque same-session `ValidatedCatalogHistory`; storage never imports IR and the proof never crosses a storage trait; ADR-0002/0004/0005/0007/0020 | Expected-version and malformed-name-registry tests; old-or-new crash; skipped/repeated/truncated/cross-session evidence rejection; proof-opacity architecture test; audit ordering and notification-after-durability |
| `ReadSnapshot` | `riffdb-storage-api`, WP-060 | WP-100 orchestration and WP-080 runtime only; catalog/query services use consumer-owned bounded read DTO ports | Synchronous, bounded, engine-neutral command reads; no storage write transaction held through runtime evaluation and no storage-API type exposed to the service; ADR-0004/0007 | Shared memory/redb conformance suite for consistent entity versions, absence, bounded prefix/epoch, ordered scans plus dependency-architecture checks |
| Compile-time read/predicate templates | `riffdb-contract-ir`, WP-040 | WP-080 and WP-100 | Every influential read statically visible; predicate plan hash/version stable; ADR-0002/0003 | Compiler negative tests for hidden/unbounded reads and semantic dependency assertions |
| Observed read evidence and checked predicate-validation request | `riffdb-storage-api`, WP-060 | WP-100 coordinates storage materialization; WP-080 consumes and preserves owned read evidence; WP-100 revalidates reads and evaluates the exact historical validation plan; WP-065 encodes durable forms | Read variants, current-value lookup, absence/version/epoch meaning, exact plan identity, validation inputs, and canonical order fixed before WP-080/WP-065. Influential range epochs prove snapshot reads; mutation-affected epochs are a separate post-validation write-planning set. Captured predicate booleans or values are not v1 storage dependencies; ADR-0003/0004 | Mutation-between-evaluation-and-commit tests for every read variant and predicate plan; overlap/difference tests for the two epoch sets; undeclared dependency, captured-result shortcut, or plan mismatch fails closed |
| `EvaluatedCommand` runtime result | Type owned by `riffdb-storage-api`, WP-060; instances constructed by `riffdb-runtime`, WP-080 | WP-100 and testkit | Contains the exact executable plan reference, checked targets/dependencies, canonical post-image mutations, ordered event intents, and declared business outcome. It contains no admission/idempotency identity, actor, provenance, logical time, partition/conflict hash, sequence, lease, storage transaction, or credential; ADR-0003/0004/0005/0007/0012 | Golden evaluated command, reference-model differential histories, determinism properties, and API checks excluding orchestration state |
| Pre-commit `CommitIntent` | Type and checked structural constructor owned by `riffdb-storage-api`, WP-060; assembled only by `riffdb-commit`, WP-100 | Coordinator write transaction and testkit | Coordinator combines the exact stored pending admission and stored admitted-provenance snapshot, a newly sourced checked `ProvenanceId`, plan-derived partition/conflict evidence, and unchanged `EvaluatedCommand`. It is self-contained but contains no assigned sequence, durable record, replay flag, lease, transaction, or semantic-validation proof; ADR-0003/0004/0005/0007/0012/0018 | Constructor mismatch tests, canonical intent fixture, provenance/admission identity assertions, source call-count/collision tests, and architecture checks proving runtime never receives admitted provenance or entropy |
| `StorageEngine`, `CommandWriteSetPlanV1`, and atomic command transaction | `riffdb-storage-api`, WP-060; memory/redb implement; WP-065 supplies codec upper-bound proofs | WP-070 and WP-100 | Must be a narrow semantic transaction, not SQL, arbitrary callback, or generic mutation API. Exact order is count-only start; admission/current-dependency recheck; private plan validation; mutation-affected target derivation/read; exact sequence-free plan; semantic and conservative encoded reservation; sequence assignment; exact graph/retained-candidate verification; actual canonical per-class envelope proof; staging. All knowable component bounds remain pre-open; only the bounded aggregate reservation uses transaction-current inputs; ADR-0003/0004/0005 | WP-060 synthetic-charge memory conformance; epoch-set distinction; capacity-before-sequence compile/type tests; WP-065 real-codec equal/one-byte-over envelope fixtures consumed by WP-070; first/max/exhausted cases; exact reservation/assignment/stage failpoints; no visible partial state or sequence gap |
| Database initialization identity and readiness | Source-free probe and atomic transition in `riffdb-storage-api`, WP-060; redb WP-070; sole production caller `DatabaseInitializationExecutor` in `riffdb-commit`, WP-100; source/lifecycle composition server WP-130 | Capability binding, idempotency identity, health/bootstrap/catalog lifecycle; WP-185 extends the accepted lifecycle | Executor returns Existing or NeedsInitialization; server generates only after Needs and passes candidate back through executor, never storage. Transition re-proves emptiness/returns a concurrent winner; reopen has no source call; invalid partial state is corrupt. Initialized remains `Initializing` until bootstrap, then only deployment until active contract; ADR-0004/0007/0009/0018/0019 | Probe/source call-count, architecture no-server-storage-mutation edge, memory/redb install/race/reopen/crash, invalid rejection, P1 process lifecycle tests |
| Structural startup evidence and operational handoff | `StructuralEvidenceSession`, bounded `HistoricalSemanticEvidence`, and dormant-port type-state owned by `riffdb-storage-api`, WP-060; memory conformance WP-060; redb `StructurallyOpened` producer WP-070; catalog-owned `ValidatedCatalogHistory` WP-050; join WP-130 | WP-130 startup/lifecycle; WP-185 reuses the resulting graph; WP-190 extends crash evidence | Initialization precedes one exclusive nonmutating session. Exact-end structural scan enumerates every authoritative namespace plus every persisted entity, index-entry, and range-prefix key as IR-opaque owner-bound evidence and yields only same-session dormant ports; catalog alone validates every historical IR reference and key against its exact retained/active schema and yields opaque same-session `ValidatedCatalogHistory`. Only WP-130 joins both plus readable inventories. No storage-to-IR edge, callback, proof-through-storage, truncation-as-success, or readiness claim in WP-070; ADR-0004/0006/0016/0019 | Memory/redb page/end/session/drop/key-coverage conformance, catalog skipped/repeated/cross-session/schema-mismatch rejection, architecture direction tests, P1 real-process graceful/restart proof |
| POC operational metadata and startup validation | Semantic retained-set interface in `riffdb-storage-api`, WP-060; exact durable inventory WP-065; redb structural validation WP-070; catalog `ValidatedCatalogHistory` WP-050; readiness composition WP-130 | WP-185 derived-component extension and WP-190 recovery | Exactly storage format version, permanent DatabaseId, independent application/administration allocator states, active-contract consistency data represented once at `catalog_active/0x01`, and singleton bootstrap marker under the accepted exact meta-key spellings. No `NodeId`, clean-shutdown marker, persisted integrity result/history, reserved field, key, type, or source port. Canonical absence is valid and complete; every startup performs the full staged read-only validation with no marker fast path or repair; ADR-0019 | Memory retained-set/absence architecture tests, WP-065 schema-inventory negative fixture, same-session structural/`ValidatedCatalogHistory` proof, graceful/crash startup call evidence, no-write checks, repeated-open idempotence |
| `ConflictKey` canonical bytes | `riffdb-types`, WP-010 | WP-040 derives expressions, WP-090 acquires, WP-100 uses | Type prefix, component encodings, sorting and dedup stable before WP-090; ADR-0003/0011 | Golden keys and ordering properties, malformed/bound tests |
| `ConflictManager` and acquired mutation capability | `riffdb-conflict`, WP-090 | WP-100 only | RAII lease is non-Clone, non-Serialize, idempotently released, all-or-nothing for sorted keys, and never enters `CommitIntent`; ADR-0003 | Loom grant/cancel/release/fairness model, Shuttle multi-key schedules, cancellation/panic cleanup |
| Idempotency identity and canonical input hash | Components in `riffdb-types` WP-010; rules in `riffdb-idempotency` WP-100; opaque storage key/admission in `riffdb-storage-api` WP-060 | WP-070 table layout, WP-100, WP-120, transports | Database/environment + authorization-resolved tenant + stable principal + contract lineage + stable command ID + keyed caller-key digest; contract version stored but excluded from lookup; pending admission has input/plan/time and no sequence; every pending/terminal scheme and key remains readable before readiness and a referenced key cannot retire in the POC; ADR-0005/0011/0012 | Cross-scope isolation, golden digests, mismatch tests, deployment retry, reservation crash/resume, three-state provider scan, missing-key readiness rejection, and retirement-block tests |
| Stored outcome vs committed outcome | `StoredOutcome` in `riffdb-storage-api` WP-060; transport-neutral `CommittedOutcome` in `riffdb-commit` WP-100 | WP-065 codec, WP-070 persistence/startup, WP-100 and WP-120 | One full `StoredOutcomeV1` envelope is both the terminal idempotency-table state and persisted outcome. The atomic commit deletes pending without a tombstone and writes no pointer or second terminal envelope. Committed form wraps the stored result with current-call `replayed` metadata. Startup validates exact outcome/commit/provenance reciprocity; ADR-0004/0005/0006 | Full-envelope and zero-byte-delete fixtures, no-pointer/no-second-envelope assertions, missing/duplicate/mismatch/still-pending startup rejection, business-rejection sequence and lost-response replay tests |
| Deterministic arithmetic and execution-fault admission | Shared pure evaluator in `riffdb-invariant`, WP-080; service preparation/disposition WP-120; runtime fault in WP-080; terminal non-commit `ExecutionFailed` admission state in `riffdb-storage-api` WP-060 and durable schema WP-065; coordinator disposition WP-100 | WP-080, WP-100, WP-120, WP-127, WP-130 | Pre-admission input-only arithmetic is root `ValidationCode::OutOfRange`, creates no pending state, and never terminalizes. After an owned snapshot exists, arithmetic/resource fault produces no `EvaluatedCommand`, `CommitIntent`, application sequence, provenance, event, or commit; before terminalization every influential dependency is revalidated. A change causes full reevaluation or leaves pending. Uncertain terminalization is `OutcomeUnknown`; proven abort is storage unavailable; ADR-0012/0013 | Pre/post-snapshot boundary fixtures, fault determinism, changed-dependency reevaluation, terminal-state/golden fixtures, failpoint distinction between proven abort and unknown commit, public error mapping, and WP-127 compatibility preservation |
| Actor, authenticated principal, capability, policy facts, decision, obligations | IDs/claim values in `riffdb-types` WP-010; raw-credential authentication and initial-authentication clock in `riffdb-auth` WP-110; operation facts, `Decision`, validated provenance/approval, and redaction/audit obligations in `riffdb-policy` WP-110 | WP-120 derives facts and authorizes every operation; WP-130/WP-140 only authenticate/map protocol input | Opaque 32-byte token, base64url text, HMAC-SHA-256 digest with key ID/version, environment/database/audience binding, trusted-vs-untrusted claims, deny default, revocation point, and field/tenant/approval obligations; ADR-0007/0009 | HMAC vectors, raw-token absence, revocation/scope/audience tests, obligation and stale-name denial matrices, secret canaries |
| Authorization clock and transaction-current capability verification | Synchronous `AuthorizationClock`, value-only mutation preparation, `TransactionCurrentCapabilityFacts`, and pure verifier owned by `riffdb-policy`, WP-110; concrete OS clock provider owned by `riffdb-server`, first composed in WP-130 | WP-120 checks current policy safe points; WP-100 consumes only the narrow preparation/facts/verifier interfaces inside capability transitions; WP-185 reuses it | Safe points sample fresh authorization time. Capability create/revoke uses one transaction-current sample for verifier facts, issue/revoke timestamp, expiry, and its administration audit. Coordinator mechanically lowers records and does not duplicate policy predicates or receive policy storage readers; ADR-0007/0009 | Exact clock-call counts, rollback/failure tests, queue-delay expiry and revocation schedules, facts/timestamp lowering equality, and architecture tests excluding storage from policy |
| Admission and administration clocks | Consumer-owned synchronous `AdmissionClock` and `AdministrationClock` ports in `riffdb-commit`, WP-100; concrete OS providers in `riffdb-server`, WP-130 | Coordinator admission/audit and compound bootstrap/catalog/control-plane paths; WP-185 reuses them | Disjoint from authentication/authorization clocks. Initial exact-facts authorization precedes durable Started; bounded permit wait precedes fresh final authorization and synchronous admission. Standalone/start/terminal/catalog/bootstrap timestamps use `AdministrationClock`; linked bootstrap facts share one sample. Clock failure is an audit outage and fails closed; ADR-0007/0009/0012 | Fake-clock call-order/count tests, queue/cancellation schedules, one-sample bootstrap linkage, clock-failure append/output-withholding matrices |
| API-neutral service requests/results | `riffdb-service`, WP-120 | WP-130 and WP-140 | No Prost/Tonic/rmcp/redb types. Service consumes the pure invariant evaluator for combined input/partition/conflict/fact preparation but no runtime execution API. Every operation takes authenticated context and obtains a policy decision; bounded cursor/wait/result types fixed before transport branches; ADR-0007 | In-process service suite, pre-admission arithmetic cases, and dependency/architecture tests proving no transport-storage or service-runtime execution edge |
| Cursor token and monotonic expiry | `CursorTokenGenerator` and `CursorMonotonicClock` consumer ports in `riffdb-service`, WP-120; OS entropy and private Instant-origin providers composed by server WP-130 | All paged service methods; transports carry only opaque tokens; WP-185 reuses the providers | Exactly 16 unpredictable bytes, bounded collision retries/capacity, process-relative 300-second expiry, checked tick regression/overflow, restart invalidation; distinct from four wall clocks and runtime; ADR-0007 | Explicit token/tick fakes, collision/capacity/expiry/rollback properties without sleeps, restart and reauthorization tests |
| Commit record, durable events, `EventHash`, provenance | Semantic DTOs plus post-assignment event preimage/hash construction and constructor validation in `riffdb-storage-api`, WP-060; exact 26-record durable Protobuf registry, complete framed hash/envelope goldens, per-class upper-bound proofs, and storage-owned codec bridge in WP-065; WP-100 supplies assigned values | WP-070, WP-160, WP-170, services | Every event has an authoritative top-level envelope plus exactly equal nested commit/outbox copies, keyed by exact 12-byte EventId; field numbers, canonical repeated order, complete post-image policy, redaction, event IDs, the exact `EventId[12] || EventTypeId:u32_be || payload_length:u32_be || canonical Value::Record` preimage under `riffdb.event/v1`, atomic graph membership, and one-way dependency from `riffdb-storage-api::proto_codec` to `riffdb-proto` fixed before WP-070; `riffdb-proto` never depends on storage; ADR-0005/0006/0011 | WP-060 constructor mismatch cases; WP-065 complete framed preimage, field-order/type/ordinal/length/cross-domain and three-copy goldens, codec round trips and actual-envelope charge proofs; WP-070 missing/orphaned/unequal/hash-invalid corruption/recovery checks; torn-write failpoints and provenance/idempotency assertions on every mutation |
| Capability repository and compound bootstrap transition | Neutral semantic DTO/port plus atomic bootstrap transition in `riffdb-storage-api` WP-060, durable message in WP-065, redb implementation WP-070; auth/policy semantics WP-110 | WP-100/WP-110/WP-120 | Stable capability ID plus digest lookup; bootstrap atomically appends the principal-less service-audit `started` record immediately before the authoritative capability/marker/lookup set, with replay linkage and no raw secret; retained credential file and containing directory are synchronized before RPC; create/revoke use coordinator operations and administration sequencing; ADR-0004/0007/0009 | Memory/redb conformance, raw-token absence, credential file/directory failpoints, crash/reopen at compound transition boundaries, replay linkage, audit ordering, revocation-next-request test |
| Durable service and administration audit | `riffdb-types` WP-010 owns closed operation/phase/ingress/link values and ADR-0021 target values; semantic `StoredServiceAuditRecordV1` and bounded append/compound transitions in `riffdb-storage-api` WP-060; durable schema WP-065; coordinator lifecycle WP-100; service target/scope/result shaping WP-120 | WP-070 recovery, WP-180 diagnostics, WP-190 process evidence | Nine lineage-scoped target tags use one canonical zero-through-16 list; service derives request-addressed targets only, while storage/transports preserve them. Mutations/admin always audit after classification; every explicit authenticated Deny audits; allowed standard reads only by obligation. Every Started selects one terminal phase and attempts one append; at most one terminal becomes durable, and outage/crash may leave none without recovery synthesis. Obligations complete before Succeeded/output. Links are closed and independent of targets; cancellation is proven, not uncertainty; ADR-0004/0007/0009/0021 | Target tag/key/permutation/duplicate/bound goldens; exhaustive 22-operation mapping and start/terminal equality; durable malformed/order recovery; exact scope/phase/link matrix, two-authorization admission order, no-audit standard-read case, replay/resume, append failure map, output withholding, compound-bootstrap and process recovery |
| Outbox repository and transition | Atomic event/intent storage port in `riffdb-storage-api` WP-060, durable codec WP-065, redb integrity WP-070; worker semantics/recovery WP-160 | WP-160, WP-185 health, and recovery | Command commit writes event plus intent but no initial status. Missing status is canonical never-attempted Pending; explicit Pending may retain retry metadata; orphan status is corrupt. WP-160 idempotently normalizes interrupted Delivering before worker readiness; authoritative WP-070 records are not repaired; ADR-0004/0005/0006 | Event/intent reciprocity and absent-status conformance, orphan rejection, fake connector duplicate/crash histories, Delivering restart normalization, no-lost-event scan |
| Projection identity, generations, state, apply markers, and frontiers | `ProjectionIdentity`/key/hash types in `riffdb-types` WP-010; group schema in `riffdb-contract-ir` WP-040; semantic control/state/apply ports in `riffdb-storage-api` WP-060; durable schema WP-065/redb authoritative checks WP-070; lifecycle/query/recovery semantics WP-170 | WP-120 service port, public schema WP-127, WP-140 resources, WP-185 health | Identity is lineage + projection ID + plan hash; generations are nonzero/never reused; every sequence gets a hash-equal apply marker; row post-images, marker, and frontier advance atomically; retired generations are inert; publication uses transaction-current head; queries fence one snapshot. Inconsistency degrades derived state without weakening authoritative readiness and rebuilds into a new generation; ADR-0010/0017 | Identity/key/hash goldens, marker-prefix/model properties, lifecycle/generation fixtures, apply/publication failpoints, continuation invalidation, degradation/new-generation rebuild and wait tests |
| Static gRPC schema boundary | Phase-zero `.proto`, common generated wire types, and five service descriptors in `riffdb-proto`, WP-020; exact execution-failure error slice in WP-010; every remaining public request/result message and schema hash in WP-127; semantic conversions in WP-130 | WP-130/client/server | WP-010's exception is limited to reviewed error symbols. WP-127 owns remaining wire structure and UUIDv7 structural validation and depends on WP-020/WP-120, not WP-065; WP-130 alone maps service DTOs to Proto. Wire types never become service DTOs; ADR-0006/0007/0009/0012/0017/0018 | Baseline/error/public descriptors and goldens, UUID wrong-length/version/variant cases, exact-value vectors, generated clean diff, gRPC conformance and limit/error tests |
| Generated MCP adapter boundary | Compiler owns transport-neutral schemas and ADR-0020 command names; service carries policy-filtered descriptors verbatim; `riffdb-api-mcp` WP-140 owns rmcp tool/resource objects, remaining resource URIs/presentation, and a narrow hosted `RequestIdSource` consumer port; WP-185 composes HTTP and supplies that port from the existing WP-130 provider | MCP transports only | WP-140 neither normalizes nor repairs command names. Remaining resource URI, audience, transport, cursor-presentation, and presentation-text rules wait for ADR-0008. MCP JSON-RPC IDs/progress tokens are distinct from RiffDB RequestId; hosted requests use injected server UUIDv7 generation; stdio uses the public Rust client; all operations invoke the shared service and reauthorize; ADR-0008/0009/0018/0020 | WP-040/WP-050 name goldens and rejection; WP-140 verbatim discovery/dispatch, golden tool/resource definitions, protocol-ID separation and UUID fixtures, structured-result validation, stale-name and both-transport auth/conformance tests |

### Accepted internal flow

The accepted command direction is:

```text
transport decode/authenticate
  -> riffdb-service constructs RequestContext, resolves/classifies the active plan,
     derives exact policy facts, and performs initial authorization
  -> required denial/standalone audit is durably appended before returning
  -> riffdb-commit durably appends Started, then waits for a bounded executor permit
  -> riffdb-service derives fresh exact facts and authorizes again
  -> riffdb-commit synchronously consumes the permit as nonretroactive admission,
     resolving or durably creating Pending with fixed tx.time
  -> riffdb-commit acquires conflict lease and materializes the bounded read set
  -> riffdb-runtime synchronously evaluates owned values and returns EvaluatedCommand
  -> riffdb-commit obtains one ProvenanceId for a successful new attempt and combines
     exact stored admission/provenance + plan evidence + unchanged EvaluatedCommand
     into the final CommitIntent
  -> coordinator performs transaction-current policy verification where required
     and opens one narrow storage transaction with a count-only candidate
  -> candidate rechecks admission and influential dependencies, validates the exact
     historical plan, derives mutation-affected prefixes, and reads their epochs
  -> candidate freezes the exact sequence-free write plan and reserves semantic plus
     conservative encoded capacity before any sequence assignment
  -> coordinator assigns the sequence, derives EventId/EventHash and the exact record
     graph, and storage verifies retained intent/plan/assignment plus actual canonical
     envelope bytes before staging
  -> storage atomically persists mutations + outcome + events + provenance + commit
  -> service fully applies redaction/filter/bound obligations to the result
  -> the one invocation-terminal audit record is durably appended
  -> transport maps the safe result
```

The service owns API admission, policy-fact derivation, authorization, durable
invocation-audit scope, policy-fact derivation, and obligations. The commit crate
owns the bounded executor permit, audit append executor, admission and clock/source
consumer ports, transaction admission from the first idempotency check through
lease release, and final `CommitIntent` assembly. Runtime owns only pure plan
evaluation and returns `EvaluatedCommand`; it never receives admitted provenance.
Storage owns mechanics and structural checks, not sequence policy, policy
predicates, or business semantics. MCP and gRPC own only protocol adaptation.

## 5. Dependency and Parallelization Strategy

### Hard-dependency waves

The following preserves every reconciled YAML dependency:

1. **Wave A:** WP-000.
2. **Wave B:** WP-010 with accepted ADR-0021's target registry.
3. **Wave C:** WP-020, WP-030, and WP-090 in parallel after their required ADRs and WP-010 interfaces freeze.
4. **Wave D:** WP-040 after WP-010/WP-030 and accepted ADR-0020. It may overlap WP-020 and WP-090, but its checked projection-schema values and versioned command-name registry must merge before WP-060 and WP-050 begin.
5. **Wave E:** WP-045 after WP-040 and WP-060 after WP-010/WP-020/WP-040 with ADR-0019's metadata boundary and ADR-0021's checked target collection already accepted. The isolated comparison work may overlap the storage interface work.
6. **Wave F:** WP-050 after WP-020/WP-040/WP-060, WP-065 after WP-020/WP-060 and accepted ADR-0019/ADR-0021, and WP-080 after WP-040/WP-060. WP-050 revalidates ADR-0020 metadata while WP-065 freezes ADR-0019's retained metadata records and ADR-0021's durable target mapping; they may run in parallel after their shared interfaces merge.
7. **Wave G:** WP-070 after WP-020/WP-060/WP-065.
8. **Wave H:** WP-075 and WP-110 after WP-070. The Fjall workspace remains isolated; WP-110 consumes the neutral capability store and policy facts.
9. **Wave I:** WP-100 after WP-050/WP-070/WP-080/WP-090/WP-110.
10. **Wave J:** WP-120 after WP-050/WP-100/WP-110.
11. **Wave K:** WP-125, WP-127, WP-160, WP-170, and WP-180 may run in parallel after their declared dependencies. WP-125 also needs WP-045; WP-127 also needs WP-020; WP-180 additionally waits for ADR-0008 acceptance.
12. **Wave L:** WP-130 after WP-020/WP-120/WP-127.
13. **Wave M:** WP-135 after WP-125/WP-130; WP-140 after WP-040/WP-120/WP-130 and acceptance of ADR-0008's remaining surface; and WP-150 after WP-130. WP-135 and WP-150 are not safe to edit the comparison tree concurrently: merge WP-135's manifest/lock/adapter work before WP-150 packages that runner. WP-140 may proceed in parallel and consumes ADR-0020 names verbatim.
14. **Wave N:** WP-185 after WP-130/WP-140/WP-160/WP-170/WP-180.
15. **Wave O:** WP-190 after WP-070/WP-100/WP-160/WP-170/WP-185.
16. **Wave P:** WP-200 after its complete declared evidence set, including WP-075, WP-135, WP-185, and WP-190.

### Interface-first merge rules

- WP-010 lands canonical types/errors, ADR-0021 targets, and fixtures before WP-020, WP-030, or WP-090 branches. No package may duplicate a semantic newtype or audit-target registry.
- WP-040 lands checked projection-group schemas and the remaining
  `riffdb-contract-ir` interfaces, including the versioned ADR-0020 command-name
  registry, before WP-060 starts; WP-050 revalidates that registry and WP-080
  consumes only merged IR.
- WP-060 lands reviewed snapshot, observed-dependency, `EvaluatedCommand`,
  `CommitIntent`, distinct influential/mutation-affected epoch sets,
  `CommandWriteSetPlanV1`, pre-sequence capacity/assignment/stage type states,
  exact EventHash constructor validation, compound-bootstrap/audit, catalog,
  capability, outbox, projection, and structural-evidence/type-state ports before
  its downstream branches. WP-070 may yield only same-session
  `StructurallyOpened` dormant ports; WP-050 alone yields the opaque historical
  `ValidatedCatalogHistory`, and WP-130 alone joins the matching pair into production
  readiness.
- WP-065 freezes durable semantic Protobuf messages, exact event-hash framed
  goldens, canonical `StoredEnvelope` codecs and conservative per-record-class
  upper-bound proofs, descriptors, and the storage-owned codec bridge before
  WP-070 persists any of those records. WP-070 must compare actual canonical
  bytes to the retained bounds before staging.
- WP-100 lands `CommittedOutcome`, the coordinator handle, and its invocation
  audit integration before WP-120 begins.
- WP-120 lands service DTOs/traits and public policy-safe result shapes before
  WP-127 fills public Protobuf messages. WP-127 merges before WP-130; WP-130 owns
  all service/Proto semantic conversions. MCP consumes the merged service and
  gRPC boundaries and does not gain a direct Proto/storage path.
- WP-045 freezes the backend-neutral budget workload/oracle before WP-125 or
  WP-135 edits the isolated comparison workspace.
- Shared changes to canonical values, IR, durable records, storage ports, policy
  facts/decisions, service DTOs, `.proto`, or MCP naming require a small interface
  PR and coordinated rebase. Durable schema changes route through WP-065 and
  public schema changes through WP-127 except the exact reviewed WP-010
  execution-failure slice. ADR-0020 command-name changes route back through
  WP-040/WP-050 rather than WP-140; parallel agents must not edit the same
  owner crate to reconcile locally invented shapes.
- WP-070, WP-100, WP-160, and WP-170 must land named test-only failpoint hooks as part of their own acceptance, because WP-190 cannot retrofit them.
- Every earlier package emits redaction-safe telemetry through a minimal stable hook or tracing contract; WP-180 supplies aggregation/rendering rather than rewriting closed transaction code.

### Gate interpretation

P0 closes only after WP-030/WP-040/WP-060/WP-080 and transitive dependencies
pass deterministic plan/model evidence. P1 explicitly includes WP-065 and WP-127
as well as WP-150, and proves reviewed durable/public schemas, the durable
standalone command path, CLI, atomic outbox intent, audit ordering,
transaction-path failpoints, and the real WP-130 `riffdbd` temporary-redb
bootstrap/deploy/budget/restart sequence. External delivery, projection workers,
MCP, observability aggregation, and the cross-component crash matrix remain P2.
P2 includes WP-185's extension of the P1 graph and closes only when POC-001
through POC-010 and all declared package evidence pass.
WP-045/WP-075/WP-125/WP-135 are evidence tracks consumed by WP-200, not alternate
semantic gates.

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

Keep this as one isolated private Rust example package, not another RiffDB semantic crate. Give it a nested standalone workspace and lockfile so PostgreSQL dependencies do not enter the root workspace lockfile or ordinary RiffDB builds; dedicated CI must still enforce Rust 1.97.0, workspace-equivalent lints, `#![forbid(unsafe_code)]`, formatting, Clippy, tests, and dependency policy. Use only the accepted PostgreSQL baseline recorded in the planning assumptions and WP-045: `postgres =0.19.14` without default features through synchronous `NoTls`, `serde_json =1.0.150` with only `std`, and the exact pinned PostgreSQL 18.4 Bookworm CI image. Any version, feature, container, native-code, or transitive-graph change requires another dependency review. SQL files are permitted only inside this comparison boundary and are outside the POC critical path.

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
- **Upstream inputs:** Workspace safety/lint policy and every required ADR, including ADR-0006 for the exact public-error slice, ADR-0013 for one-based stable semantic IDs, ADR-0021 for exact service-audit targets, and ADR-0011/0014/0016/0017 for canonical identity, hash, key, and projection boundaries.
- **Downstream interfaces:** `riffdb-types` newtypes/value semantics and `riffdb-errors` safe error envelope; an inventory must include every cross-crate ID shown in the SPEC, not only the illustrative list. All compiler-assigned IDs plus application `ContractVersion` are one-based/nonzero by construction. The package also lands ADR-0021's nine target variants/tags and canonical checked collection, plus the exact reviewed ADR-0012 execution-failure Proto symbols, exhaustive mapper/preflight, and generated goldens, with no other public or durable schema change.
- **Principal risks:** Platform-dependent bytes, decimal ambiguity, Unicode ambiguity, hash-domain collision, ambiguous lineage-local audit IDs, derived-enum ordering leaking into a durable boundary, unbounded values, secrets in errors, and later packages duplicating actor/event/index IDs.
- **Acceptance evidence:** Types/errors/Proto tests and Clippy, deterministic Proto regeneration, golden encoding/hash/error-wire vectors, ADR-0021 tag/key/permutation/duplicate/16-versus-17/different-lineage tests, and property tests for ordering, zero rejection, bounds, checked arithmetic, scale, and redaction.
- **Human-review triggers:** Any durable encoding, hash algorithm/domain, identifier representation, timestamp meaning, public error class, or persisted actor/provenance field.
- **Parallelism:** None until the interface commit merges.
- **Recommended PR boundary:** First commits for nonzero newtypes and bounded semantic vocabulary; projection/hash types next; then one atomic domain-error plus exact Proto-slice commit. Do not mix any other public field into the carve-out.

#### WP-020 — Protobuf and durable envelope

- **Purpose:** Define the reviewed phase-zero v1 common value/error/envelope boundary, fixed service/RPC descriptors, and pure-Rust reproducible generation pipeline that later schema-owner packages extend.
- **Hard dependencies:** WP-010.
- **Upstream inputs:** Canonical values/errors, canonical gRPC service decision, phase-zero durable/public inventory, and ADR-0006.
- **Downstream interfaces:** Foundational `.proto` sources, generated `riffdb-proto` types, bounded wire-structural/value conversion rules, envelope version/checksum/schema policy, fixed service/RPC identities, reserved fields, and baseline golden descriptors/records consumed by WP-065 and WP-127.
- **Principal risks:** Prematurely freezing later semantics, conflating wire and storage compatibility, lossy exact decimals, unknown-field loss, or allowing generated types into core services.
- **Acceptance evidence:** `cargo test -p riffdb-proto`, exact `generate-proto --check`, descriptor/old-record fixtures, size-limit tests, and decoder fuzz target registration for later fuzz packages.
- **Human-review triggers:** Any public or durable field number, record type, envelope checksum/hash meaning, compatibility class, or generator dependency.
- **Parallelism:** Safe with WP-030 and WP-090 after WP-010 freezes.
- **Recommended PR boundary:** Completed phase-zero schema/interface review first; generator and foundational conversions second; compatibility fixtures last. Later durable/public completion belongs only to WP-065/WP-127.

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
- **Upstream inputs:** Frozen AST/source spans, canonical values/IDs, ADR-0002, the decision for deterministic bundle metadata, and ADR-0020's exact command tool-name grammar and ownership.
- **Downstream interfaces:** `ContractBundle`, typed HIR, forward-only `CommandPlan`, plan/hash/version metadata, schema IR, read/predicate templates, immutable checked `ProjectionGroupSchema`/`BoundProjectionGroupSchema` values consumed narrowly by WP-060, and a versioned CommandId-ordered command-name registry consumed by WP-050/WP-120/WP-140.
- **Principal risks:** Unstable numeric IDs/hash, AST evaluation at runtime, hidden dependencies, unsupported invariant acceptance, transport-specific metadata beyond the accepted name, nondeterministic collections/timestamps, or accepting invalid/colliding names for repair downstream.
- **Acceptance evidence:** Stable budget IR/plan hash/JSON Schemas/docs, compatibility and explain snapshots, ADR-0020 grammar/128-byte/case-collision/order/hash/source-span fixtures and properties, invalid-corpus diagnostics, repeated-build equality, and generator clean check.
- **Human-review triggers:** New language control flow/invariant class, stable ID rule, IR/durable change, conflict/locality rule, hidden read, plan-hash input change, or command-name registry/version/compatibility change.
- **Parallelism:** May overlap WP-020/WP-090, but WP-060 cannot begin until the checked projection-schema interface merges.
- **Recommended PR boundary:** IR interface and fixtures; compiler passes; dependency/conflict/invariant analysis; schema/explain/generation.

#### WP-045 — Budget comparison baseline

- **Purpose:** Establish the long-lived annual-budget workload, normalized observation oracle, explicit guarantee profiles, and isolated PostgreSQL implementation.
- **Hard dependencies:** WP-040.
- **Upstream inputs:** Canonical budget contract/IR semantics, exact values, outcome definitions, and deterministic fixtures.
- **Downstream interfaces:** Backend-neutral workload and observations, golden oracle fixtures, PostgreSQL adapter/schema, and correctness preflight consumed by WP-125/WP-135/WP-200.
- **Principal risks:** Treating PostgreSQL as the oracle, leaking SQL/driver dependencies into RiffDB, comparing unequal guarantees, or timing an incorrect implementation.
- **Acceptance evidence:** The nested workspace test command, deterministic fixture regeneration, and a report of PostgreSQL version, isolation, durability, and concurrency assumptions.
- **Human-review triggers:** PostgreSQL driver/server dependency, SQL outside the isolated workspace, guarantee-profile change, workload semantic change, or a shared abstraction that resembles storage CRUD.
- **Parallelism:** Safe with WP-060 after WP-040; it must not edit root workspace semantics. Later WP-050/WP-065/WP-080 branches wait for their declared inputs.
- **Recommended PR boundary:** One isolated workload/oracle/PostgreSQL PR; do not include any RiffDB adapter yet.

#### WP-060 — Storage semantic API

- **Purpose:** Define the narrow engine-neutral snapshot, observed-dependency, evaluated-command/intent DTOs, atomic command, catalog, capability/bootstrap, service-audit, outbox, projection, scan, integrity, startup structural-evidence/type-state, and backup semantics plus a memory reference engine.
- **Hard dependencies:** WP-010, WP-020, and WP-040.
- **Upstream inputs:** Canonical values/keys, phase-zero envelopes, immutable checked projection-group schemas, accepted transaction/storage/service/capability/projection ADRs, ADR-0019's exact retained metadata set, and ADR-0021's checked target collection.
- **Approved narrow scope exception:** WP-060 may edit only `crates/riffdb-types/src/codec.rs` outside its original crate set to add a borrowed canonical-record encoder. The helper must produce byte-for-byte canonical value v1 output, must not change any tag, bound, ordering, error compatibility, or durable format, and exists only to establish the aggregate bound before storage code duplicates an owned record.
- **Downstream interfaces:** Source-free database-identity probe/initialization, exactly the six retained metadata categories with canonical absence of all deferred items, `StructuralEvidenceSession`, bounded exact-end `HistoricalSemanticEvidence`, same-database/session type-state and `StructurallyOpened` dormant-port handoff, `ReadSnapshot`, `EvaluatedCommand`, checked `CommitIntent` including ProvenanceId, distinct influential and mutation-affected epoch collections, exact sequence-free `CommandWriteSetPlanV1`, pre-sequence capacity type states, post-assignment `EventHash` construction/constructor validation, ordered scan, atomic records, compound bootstrap and standalone audit transitions preserving checked targets without derivation, event/intent plus absent-status semantics, specialized repositories, separated integrity findings/readiness, and shared conformance suite.
- **Principal risks:** Generating identity in storage or on reopen, inventing a `NodeId`/shutdown marker/integrity history placeholder, deriving or reordering audit targets in storage, conflating influential and mutation-affected epochs, assigning a sequence before exact aggregate reservation, trusting an upper bound without checking actual canonical envelopes, hashing only an event payload, treating truncation as exact end, releasing ports before scan completion, allowing mutation during evidence, importing or interpreting historical IR in storage, passing `ValidatedCatalogHistory` through storage, giving storage sequence/business/policy ownership, holding a transaction across runtime, generic callbacks/transactions, redb leakage, reversed IR/Proto dependencies, provenance entering runtime, incorrect audit/outbox semantics, or omitted persistence needs.
- **Acceptance evidence:** Memory suite proving source-free Existing/NeedsInitialization and atomic initialization races; initialization-before-evidence; exclusive noninterleaving session behavior; bounded continuation, exact-end, drop/failure, and database/session mismatch cases; dormant ports that cannot self-activate; exact retained metadata plus valid absence/architecture rejection of deferred items; checked target preservation/bounds; snapshot/bounds; evaluated-command/intent/provenance checks; epoch-set overlap/difference; count-only start and capacity-before-sequence type states; synthetic equal/over-charge staging with no dependency on the later durable codec; exact event-hash preimage and mismatch rejection; atomicity; no-gap sequence/log ordering; idempotency; catalog CAS; audit/bootstrap ordering/failure classes; reciprocal event/intent with implicit Pending; separated derived findings; projection generation/frontier; and model equality.
- **Human-review triggers:** Transaction ordering/lifetime, revalidation responsibility, key/record encoding, sequence allocation, generic mutation ability, or any path that resembles arbitrary transaction callbacks.
- **Parallelism:** Begins only after WP-040's checked projection schema merges. Its interface must merge before WP-050/WP-065/WP-080 implementations branch.
- **Recommended PR boundary:** ADR-backed trait/record interface; memory implementation; shared semantic/model properties.

#### WP-080 — Deterministic command runtime

- **Purpose:** Own the shared pure checked-expression evaluator for predicates, invariants, postconditions, and commit checks, then synchronously interpret compiler-produced IR against an owned snapshot and return a deterministic `EvaluatedCommand` with complete read evidence and canonical mutation/event intent. Contract state-machine source, IR, and execution are outside POC grammar/IR v1.
- **Hard dependencies:** WP-040 and WP-060.
- **Upstream inputs:** Frozen plan, canonical value/logical-time and checked-ID rules, `ReadSnapshot`, observed-dependency types, and ADR-0012/ADR-0018 no-runtime-source decisions.
- **Downstream interfaces:** `riffdb-invariant` pure input-computable expression plus predicate/invariant/postcondition/commit-check evaluator, value-only transaction context, snapshot interpreter, construction of storage-API-owned `EvaluatedCommand`, and generated-history adapters. WP-120 owns combined request preparation; WP-100, not runtime, assembles final `CommitIntent` from durable admission/provenance.
- **Principal risks:** Hidden clock/random/UUID source/global state, UUID-derived time/order, `.await` or I/O, missed reads/predicates/checks, accidental state-machine placeholder or transition dispatch, noncanonical mutation/event ordering, input-buffer references, or divergent reference semantics.
- **Acceptance evidence:** Runtime/invariant tests, same-input/snapshot/time equality, generated histories, reference-model differential comparison, pre-admission-versus-snapshot arithmetic boundary cases, negative corpus and architecture assertions excluding contract state-machine AST/HIR/IR/instructions/dispatch, and checks that runtime receives checked RequestId only and cannot receive a combined preparation DTO, provenance/incident/UUID providers, entropy, clocks, or storage capability.
- **Human-review triggers:** Any randomness, time source, dynamic read, retry behavior, new instruction/control flow, commit-check representation, or invariant interpretation change.
- **Parallelism:** Safe with WP-050/WP-070 once WP-040/WP-060 interfaces merge.
- **Recommended PR boundary:** Evaluator/context interface; expression/instruction engine; dependency/invariant handling; differential histories.

#### P0 gate evidence

P0 closes only after identical contract inputs produce identical semantic IR,
plan hashes, JSON Schemas, `EvaluatedCommand` values (or the same closed
deterministic execution fault), and reference-model histories. “Generated MCP
metadata” at this gate means transport-neutral command metadata and schemas;
protocol-specific MCP objects remain WP-140.

### 6.3 P1 — Durable Standalone Database

#### WP-050 — Contract catalog

- **Purpose:** Persist immutable bundles, validate every stored historical plan for one startup session, and atomically activate compatible expected versions with post-durability notifications.
- **Hard dependencies:** WP-020, WP-040, and WP-060.
- **Upstream inputs:** Versioned bundle encoding, compatibility report, ADR-0020 command-name registry and derivation, catalog repository/CAS semantics, and typed coordinator administrative-operation boundary.
- **Downstream interfaces:** Active catalog snapshot, deployment request/result, immutable lookup by version/hash, reject-without-repair registry revalidation, bounded historical evidence resolver/validator, opaque catalog-owned `ValidatedCatalogHistory` bound to the same database/open session, expected-version behavior, and notification stream. The proof never crosses storage.
- **Principal risks:** Torn bundle/pointer, destructive compatibility, active plan/hash mismatch, accepting missing/duplicate/reordered/misbound/noncanonical command-name metadata, accepting skipped/repeated/truncated/cross-session evidence, moving IR validation or a callback into storage, notification before durability, or bypassing coordinator policy.
- **Acceptance evidence:** Catalog tests, ADR-0020 malformed-registry rejection matrix, complete historical bundle/plan resolution, exact-end/page-order/session-mismatch rejection, proof-opacity and no-storage-IR architecture tests, expected-version races, compatibility fixtures, and old-or-new activation recovery. Process-kill durability must use a durable backend when available.
- **Human-review triggers:** Active-pointer ordering, bundle/storage format, destructive change, catalog sequencing, or notification visibility.
- **Parallelism:** Can overlap WP-070/WP-080 after WP-060 interface; cannot claim durable crash proof before redb support.
- **Recommended PR boundary:** Catalog interfaces/results; compatibility/activation; persistence/recovery; notifications.

#### WP-065 — Durable semantic record schema

- **Purpose:** Freeze the exact versioned Protobuf representation of the semantic durable records accepted by WP-060 before any production backend persists them.
- **Hard dependencies:** WP-020 and WP-060.
- **Upstream inputs:** Phase-zero envelope/value policy, storage-owned semantic DTOs and bounds, the accepted eight-module/26-record registry, exact ADR-0011 event preimage, sequence-free write-set charge classes, ADR-0019's exact retained metadata inventory, ADR-0021's nine targets/canonical order, projection identity/generation/frontier records, service-audit records, capability/bootstrap records, and every atomic command record.
- **Downstream interfaces:** The exact `common`, `metadata`, `application`, `catalog`, `capability`, `audit`, `outbox`, and `projection` semantic-record Proto modules in addition to the existing envelope module; 26 registered envelopes including top-level `StoredDurableEventV1`; both `Next(nonzero) | Exhausted` allocator states and the reviewed target representation; exact event-hash codec and complete framed goldens; exact actual canonical `StoredEnvelope` charge reporting and conservative per-record-class upper-bound proofs; descriptors, schema hashes, golden old/current bytes, structural wire validators, a generated inventory proving no deferred-metadata record/field/key/envelope placeholder, and the one-way `riffdb-storage-api::proto_codec` semantic codec bridge. `riffdb-proto` never depends on storage API.
- **Principal risks:** Guessing field numbers before semantic DTOs freeze, encoding `StoredOutcomeV1` as a pointer or second terminal record, charging pending deletion as a tombstone, defining an upper bound that the real codec can exceed, hashing Protobuf or payload-only event bytes, losing ADR-0021 tags/order or repairing malformed targets, reserving speculative `NodeId`/shutdown/integrity-history fields, reversing the foundational dependency, conflating public and durable messages, lossy unknown-field handling, or letting generated Prost types escape into runtime/service/policy APIs.
- **Acceptance evidence:** `cargo test -p riffdb-proto -p riffdb-storage-api`, deterministic proto generation check, durable descriptor/golden and negative schema-inventory diffs including all 26 types, all target variants/order/malformed cases, and first/max/exhausted allocator and compound-capacity cases; full-`StoredOutcomeV1` terminal-row, no-pointer/no-tombstone/no-second-envelope, and malformed outcome/commit/provenance reciprocity fixtures; complete framed event-hash vectors and three-copy mismatch cases; real-codec equal-bound/one-byte-over/aggregate reservation fixtures proving deletes have zero encoded but nonzero semantic charge; semantic round trips, malformed/limit tests, and decoder-fuzz corpus registration.
- **Human-review triggers:** Any durable message/field number, envelope/key/codec change, compatibility classification, unknown-field policy, or dependency-direction change.
- **Parallelism:** May run with WP-050/WP-080 after WP-060; WP-070 must wait for it. It does not own public API messages or service conversions.
- **Recommended PR boundary:** One interface-first durable-schema PR: messages/descriptors and review fixtures, then storage-owned codecs and negative compatibility evidence. Do not mix redb mechanics into it.

#### WP-070 — Redb storage engine

- **Purpose:** Implement all frozen semantic storage ports using redb, the accepted canonical table/key layout, atomic command records including standalone events, complete structural startup evidence with dormant opened ports, recovery/integrity, SHA-256 offline backup/restore, and dependency-free component benchmarks.
- **Hard dependencies:** WP-020, WP-060, and WP-065.
- **Upstream inputs:** Frozen storage traits/semantic DTOs and exact candidate order, reviewed durable messages/envelopes/codecs, lineage-framed bundle keys, one active-catalog row, exact canonical entity/index/range keys, authoritative standalone events, per-record-class envelope upper bounds, exact event-hash fixtures, ADR-0019 retained metadata and canonical absence rules, ADR-0021 target validation, durability modes, and named failpoint protocol.
- **Downstream interfaces:** Concrete engine/configuration hidden behind storage API, exclusive structural-evidence session, complete exact-end historical pages, same-session `StructurallyOpened` dormant ports, recovery/integrity report, backup manifest, and failpoint hooks for WP-190. It exposes no IR-aware or operational-readiness proof.
- **Principal risks:** Redb type leakage, sequence assignment before capacity, storing an outcome pointer/duplicate terminal row/pending tombstone, charging deleted envelopes as encoded writes, a codec upper-bound excess discovered after staging, outcome/commit/provenance or event three-copy reciprocity drift, sequence visibility before durability, incomplete specialized tables, format ambiguity, accepting or repairing unknown/duplicate/noncanonical audit targets, a marker/history fast path that weakens startup validation, treating a bounded/truncated scan as complete, releasing mutation ports during validation, importing catalog/IR, claiming readiness from structure alone, writing deferred operational metadata, unsafe repair, dependency creep, or engine-specific semantics becoming public.
- **Acceptance evidence:** Shared engine conformance, package properties, and a
  process recovery matrix covering both sequence spaces and exhaustion metadata,
  pending/terminal idempotency digest-provider availability, full-outcome row
  ownership and exact outcome/commit/provenance reciprocity, capability lookup/
  bootstrap/audit cross-links, exact event-hash and reciprocal commit/event/outbox-intent linkage,
  standalone audit with exact target integrity, compound bootstrap/replay,
  verified backup/restore, complete read-only structural validation and historical
  evidence through exact end after both graceful close and crash, dormant-port
  session binding with no catalog/IR dependency or readiness claim, no deferred
  metadata write, and benchmark compilation. Mismatch fails the open;
  no POC online sequence repair or referenced-key retirement is accepted. The
  matrix also covers capacity failure before sequence, exact-bound staging,
  one-byte-over canonical-envelope rejection before staging, and every boundary
  through assignment/graph verification/commit.
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

- **Purpose:** Own transaction admission after policy/plan resolution: idempotency, conflict acquisition, runtime evaluation, influential revalidation, mutation-affected epoch planning, pre-sequence capacity reservation, sequence-derived event/record construction, atomic persistence, replay, and lease release.
- **Hard dependencies:** WP-050, WP-070, WP-080, WP-090, and WP-110.
- **Upstream inputs:** Engine and atomic transaction, pure evaluator returning `EvaluatedCommand`, conflict lease, catalog/commit-check plan, stored pending admission/provenance, checked ADR-0021 target lists, policy-owned value-only capability preparation/facts/verifier, injected admission/administration clocks and provenance source, failpoints, and approved idempotency/audit semantics.
- **Downstream interfaces:** Bounded coordinator handle/queue and permit; commit-owned `DatabaseInitializationExecutor`, `AdmissionClock`, `AdministrationClock`, `ProvenanceIdSource`, and audit executor; idempotency derivation/reservation, checked final `CommitIntent`, `CommittedOutcome`, typed replay/mismatch/failure mapping, and post-durability notification.
- **Principal risks:** Business work before durable Started, changing/rederiving target lists in the executor, conflating influential dependencies with affected epochs, stale final authorization after queue wait, duplicate effects or provenance after uncertainty, identifier collision after sequence allocation, capacity failure after sequence, wrong EventHash preimage, wrong identity scope, unstable `tx.time`, runtime entropy/provenance leakage, sequence gaps, duplicated policy predicates, missing revalidation, incorrect audit terminal/link, output before terminal durability, cancellation after an authoritative result, or lease release failure.
- **Acceptance evidence:** Package/backpressure tests; initialization probe/candidate flow proving commit is sole production transition caller; exact initial-auth/Started/permit/fresh-auth/admit order; target preservation; clock/source calls; intent assembly; epoch-set distinction; capacity-before-sequence and retained-plan/assignment equality; exact ordinal/EventId/EventHash graph; provenance collision/replay/unknown schedules; capability tests; exhaustive audit map/links; budget race; revalidation; reservation/assignment/envelope/stage failpoints; and fail-after-commit replay with one mutation/event/record/sequence/provenance ID.
- **Human-review triggers:** Identity tuple/reservation, transaction ordering, predicate source, sequence semantics, atomic set, admission ownership, cancellation boundary, or outcome persistence.
- **Parallelism:** Starts only after WP-110; it may overlap no package that is still changing its catalog, storage, runtime, conflict, or principal inputs. Do not start WP-120 until its public result freezes.
- **Recommended PR boundary:** Identity/outcome interfaces; coordinator queue/state machine; atomic engine integration; concurrency/idempotency/failpoint evidence.

#### WP-110 — Authorization and capability service

- **Purpose:** Create and resolve opaque local capability tokens, authenticate principals, define policy facts and fresh authorization time, enforce deny-by-default policy including transaction-current capability verification, and produce redaction/tenant/field/approval/audit obligations.
- **Hard dependencies:** WP-010 and WP-070.
- **Upstream inputs:** Canonical actor/capability IDs and pure UUIDv7 assembler, neutral capability repository and compound bootstrap transition, reviewed HMAC/audience/entropy dependency semantics, safe initial-authentication and authorization clock boundaries, and typed coordinator administrative operations.
- **Downstream interfaces:** Auth-owned raw-credential wrapper, `AuthenticatedPrincipal`, credential authenticator and bootstrap-secret helper; policy-owned operation facts, `Decision`, obligations/redaction plan, validated claims, `AuthorizationClock`, value-only mutation preparation, `TransactionCurrentCapabilityFacts`, and pure transaction-current verifier.
- **Principal risks:** Reusing bootstrap entropy between CapabilityId and token, regenerating retained bootstrap credentials after uncertainty, incorrectly promising recovery of a lost normal-create token, raw token persistence/logging, cross-environment/audience replay, timestamp drift between verifier/state/audit, stale revocation, trusting claims, duplicated predicates, policy storage access, clock rollback, or visibility treated as authorization.
- **Acceptance evidence:** Separate bootstrap entropy-fill/zeroization failures, bootstrap ID/token retention with fresh RequestId, normal-create equal-ID `AlreadyCreatedTokenUnavailable`, token secrecy, scope/environment/audience, exact one-sample expiry/issue/revoke/audit behavior, clock failure/rollback, queue-delay revocation, facts/verifier equality, tenant/field/approval, compound bootstrap, visibility, and non-bypass tests.
- **Human-review triggers:** Cryptography/provider, token entropy/hash, administrative capability, approval, revocation point, audience, field policy, redaction, or persistence path.
- **Parallelism:** Safe with WP-075 after WP-070. WP-100 waits for its principal/capability interfaces.
- **Recommended PR boundary:** Token/store interface; authentication lifecycle; policy/obligations; cross-path security tests.

#### WP-120 — API-neutral application service

- **Purpose:** Expose one transport-free, policy-filtered command/contract/entity/commit/provenance/projection/health surface with durable invocation-attempt audit.
- **Hard dependencies:** WP-050, WP-080, WP-100, and WP-110.
- **Upstream inputs:** Catalog, the shared pure `riffdb-invariant` evaluator, coordinator, auth/policy, query/storage ports, safe DTO/error types, compiler-owned ADR-0020 command descriptors, ADR-0021 target vocabulary, and optional derived/health service ports.
- **Downstream interfaces:** Request/bootstrap contexts, combined input/partition/conflict/fact preparation with pre-admission arithmetic mapped to root `ValidationCode::OutOfRange`, closed service traits, exact request-target derivation, policy-filtered descriptors carrying compiled command names verbatim, bounded cursor/page/wait types plus token/monotonic-clock ports, transport-neutral outcomes, two-authorization admission, closed audit rules, and in-process harness.
- **Principal risks:** Transport/storage leakage, hidden generic mutation, authentication implying authorization, service-side command-name derivation/repair, missing/extra/result-expanded audit targets, facts drifting between checks, denial/read scope errors, replay/resume selecting or attempting the wrong terminal count, terminal synthesis after outage, uncertainty mislabeled cancellation, protected output before terminal durability, late obligations, unbounded scans/waits, or secret leakage.
- **Acceptance evidence:** All-operation service suite; verbatim compiled-name descriptors and no-normalizer architecture check; exhaustive 22-operation target mapping with start/terminal equality and no authorizing/result/traversed/returned expansion; mutation/admin/Deny/obligated-read scope; initial-auth/Started/permit/fresh-auth sequence; one-selected/attempted and at-most-one-durable terminal matrix including no-visible-terminal outage/crash; append-failure result map; shaping before Succeeded; stream-establishment-only audit/later reauth close; telemetry boundary; cursor fencing; and non-bypass checks.
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

#### WP-127 — Public Protobuf API completion

- **Purpose:** Fill the remaining phase-zero public request/result shells only after WP-120 freezes API-neutral semantic DTOs, preserve the WP-010 execution-failure error slice, and avoid making generated messages the service model.
- **Hard dependencies:** WP-020 and WP-120.
- **Upstream inputs:** Phase-zero service/RPC descriptors and compatibility policy; closed service requests/results, public-safe errors, projection lifecycle/frontier results, capability create/revoke modes, cursors, pages, waits, and audit-safe administration shapes.
- **Downstream interfaces:** Complete public `.proto` messages, field reservations, descriptors, schema hashes, golden wire fixtures, UUIDv7 structural validation, and other wire-structural validators. It owns no durable schema, identifier generation, or service/Proto semantic conversion.
- **Principal risks:** Conflating durable and public records, silently changing RPC identity, assigning fields without closed DTO semantics, putting policy decisions or secrets on the wire, or adding a direct WP-065 dependency that collapses the boundary.
- **Acceptance evidence:** `cargo test -p riffdb-proto`, deterministic generation, descriptor/schema-hash/golden fixtures, UUID wrong-length/version/variant and ADR-0018 golden tests, public limit/unknown-tag tests, and explicit evidence that semantic conversions remain absent until WP-130.
- **Human-review triggers:** Any public message or field number, service/RPC change, compatibility classification, public error/lifecycle tag, cursor carriage, or generated-artifact hash change.
- **Parallelism:** May run with WP-125/WP-160/WP-170/WP-180 after WP-120; WP-130 waits for it. It does not depend directly on WP-065.
- **Recommended PR boundary:** One public-schema interface PR with descriptor/golden review and structural validation; leave Tonic/service conversions to WP-130.

#### WP-130 — gRPC server and Rust SDK

- **Purpose:** Map the approved public protocol onto the service, compose the minimum runnable production `riffdbd` over redb, and provide generic/generated Rust clients with safe same-key retry.
- **Hard dependencies:** WP-020, WP-120, and WP-127.
- **Upstream inputs:** Frozen complete public `.proto`; service DTOs; redb `StructurallyOpened` dormant ports and exact-end historical evidence; catalog-owned opaque same-session `ValidatedCatalogHistory`; commit initialization/provenance ports; auth/policy/service implementations; narrow `CredentialAuthenticator` handoff including exact bootstrap metadata extraction; disjoint clock/cursor/digest consumer ports and readable-inventory requirements; safe errors; server lifecycle/configuration; and later-worker extension ports.
- **Downstream interfaces:** Tonic services/interceptors and sole service/Proto conversions; the minimal production redb/catalog/commit/auth/policy/service component graph; staged initialization/structural/catalog/digest/lifecycle readiness gate; one private checked server UUID system-source primitive with Request/Incident/Database/Provenance wrappers; four disjoint wall-clock providers; cursor token/monotonic providers; disjoint capability/idempotency digest providers and inventories; transport client and convenience RequestId/CapabilityId/AgentSessionId source; generated contract module; retry/error mapping; and stable P2 extension ports.
- **Principal risks:** Business outcomes as status errors, adapter-side semantics, direct semantic bypass, constructing principals in the adapter, activating storage ports before matching same-session `ValidatedCatalogHistory`, treating structural validation as readiness, server-to-storage initialization mutation, UUID source/global-state drift, mixed clocks or digest material, cursor time leaking into semantics, invented retry keys, conflating retry submission IDs with idempotency identity, bootstrap replay regenerating identity/token, protocol limit bypass, secrets in config, or rebuilding the P1 graph in WP-185.
- **Acceptance evidence:** Package tests; UUID/clock/entropy/collision call-count tests and direct-owner feature graph; four-clock and cursor-provider separation; digest inventory/reuse rejection; initialization only through `DatabaseInitializationExecutor`; no DatabaseId source call on reopen; exact-end structural/`ValidatedCatalogHistory` same-session proof and mismatch/drop cases; malformed/limit/auth/status conformance; command/outcome/deploy/entity E2E; fresh-RequestId same-key retry; retained bootstrap replay; generated signature fixture; all projection variants against an injected service stub; and a real child-process `riffdbd` test on temporary redb that performs bootstrap, deploy, `CreateBudget`, entity read, `AllocateBudget`, outcome resolution, clean stop, reopen, durable rereads, and bootstrap replay through public gRPC only.
- **Human-review triggers:** Public schema/status/retry change, auth interceptor, server configuration/privilege boundary, transport limit, or composition surface.
- **Parallelism:** May overlap WP-160/WP-170/WP-180 after composition ports; should precede WP-140 under the specified stdio design.
- **Recommended PR boundary:** Wire conversions/conformance; minimal production composition/providers/startup gate; child-process P1 restart evidence; generic client; generated module. Do not include MCP or derived workers.

#### WP-135 — Public SDK comparison adapter

- **Purpose:** Make the Rust SDK over public gRPC the canonical RiffDB adapter for the long-lived comparison runner.
- **Hard dependencies:** WP-125 and WP-130.
- **Upstream inputs:** Workload/oracle, service comparison baseline, public Rust client, gRPC authentication, retry/error/outcome mapping, and process lifecycle.
- **Downstream interfaces:** Public-only comparison adapter, same-key uncertain-response scenario, normalized process observations, and correctness preflight consumed by WP-200.
- **Principal risks:** Falling back to in-process internals, client-generated semantic behavior, benchmark transport mismatch, or divergence from WP-125 observations.
- **Acceptance evidence:** The exact nested-workspace `public_comparison` command, same-key recovery proof, and equality with normalized oracle observations before timing.
- **Human-review triggers:** Any non-public import, retry identity change, guarantee-profile change, benchmark topology change, or production API altered for the example.
- **Parallelism:** Safe with WP-140 after WP-125 and WP-130. Do not run comparison-tree edits in parallel with WP-150; merge this package's manifest/lock/adapter work first.
- **Recommended PR boundary:** One public SDK/gRPC adapter and process correctness PR.

#### WP-150 — CLI and local developer flow

- **Purpose:** Provide public-API-only operator/demo commands with stable machine JSON and additive human output.
- **Hard dependencies:** WP-130.
- **Upstream inputs:** Rust client, public auth/capability operations, auth-owned pure bootstrap-secret generation/validation helper, error/retry semantics, configuration precedence, stable service operations, and budget comparison runner packaging contract.
- **Downstream interfaces:** `riffdb` command tree, bounded config/output model, local token workflow, and dry-run demo commands. Its sole non-public dependency exception is the pure bootstrap-secret helper; every database operation still uses loopback public gRPC.
- **Principal risks:** Direct storage/policy/authenticator shortcut, widening the bootstrap helper, secrets in argv/output/config, sending bootstrap before retained credential durability, regenerating bootstrap ID/token after uncertainty, promising normal-create token recovery, reusing a RequestId, unstable JSON, unsafe retry, destructive restore defaults, or forking demo logic.
- **Acceptance evidence:** CLI package tests, JSON snapshots/semantic assertions,
  public API integration, credential file/close/containing-directory sync
  failpoints proving no pre-durability RPC, uncertain bootstrap retry retaining
  ID/token with a fresh RequestId, normal-create token-unavailable handling,
  dependency-architecture proof that only the pure auth helper is imported, and
  `scripts/demo --dry-run`.
- **Human-review triggers:** Privileged bypass, credential input, destructive operation, stable JSON change, or a command unavailable through public API.
- **Parallelism:** Safe with WP-140/WP-160/WP-170/WP-180 after WP-130. Comparison-runner packaging waits for WP-135 to merge and treats its manifest, lockfile, adapters, fixtures, and guarantee profiles as read-only.
- **Recommended PR boundary:** Command/config/output framework; contract/command/read/admin flows; comparison-runner demo wiring/tests.

#### P1 gate evidence

The durable gate must include WP-065 and WP-127 and show reviewed durable/public
schema fixtures, one authoritative atomic command set, sequence/revalidation
correctness including mutation-affected epoch reads and capacity-before-sequence,
actual canonical-envelope bound checks, exact EventHash construction/recovery,
same-key uncertain-response recovery, compound-bootstrap and
invocation-audit ordering, transaction-current authorization, deny-by-default
shared service access, stable database/provenance identity, transaction-path
process recovery, and the public-API-only CLI. Atomic event/outbox intent with an
implicit initial Pending status is part of the WP-070/WP-100 record set; external
dispatch and the full outbox/projection crash matrix remain P2.

The **first runnable database checkpoint** is the P1 close. Its smoke sequence is
concrete: start `riffdbd` on a temporary redb database; create and durably retain
the bootstrap credential; deploy the canonical budget contract; execute its
ordinary `CreateBudget` command; fetch the created budget through the public
entity-read API; execute `AllocateBudget`; fetch the changed entity and resolve
the persisted outcome; stop the process cleanly; restart it against the same
database; fetch the same entity and outcome again through the CLI/gRPC path; and
repeat bootstrap with the retained credential to recover the original capability
result without echoing or reissuing the token while appending only the required
linked replay-audit records. The WP-130
child-process test automates the entire public gRPC bootstrap/deploy/command/read/
resolve/restart/replay sequence over real redb, and WP-150 makes every step
available through the public CLI. This checkpoint does not wait for MCP, outbox
dispatch, projection workers, observability aggregation, or WP-185's P2
extension, and it must not use an in-process storage or service bypass.

### 6.4 P2 — Native MCP and POC Exit

#### WP-140 — Native MCP interface

- **Purpose:** Expose authorized active-contract commands as dynamic tools and database context as filtered resources over stdio and loopback Streamable HTTP.
- **Hard dependencies:** WP-040, WP-120, and WP-130.
- **Upstream inputs:** Contract schemas/plans and accepted ADR-0020 names carried verbatim by service DTOs, policy/redaction decisions, gRPC client, the remaining ADR-0008 URI/transport/presentation decision, HTTP audience binding, injected hosted RequestId source, and server registration hook.
- **Downstream interfaces:** Protocol-isolated rmcp adapter, hosted RequestId consumer port, session catalog, fixed tools/resources, structured result mapping, pagination/progress/cancellation, subscriptions/list-change behavior.
- **Principal risks:** Storage/service bypass, adapter-side command-name normalization/repair, stale-name execution, schema mismatch, conflating MCP protocol IDs or progress tokens with RiffDB RequestId, adapter-generated idempotency key, catalog overexposure, unsafe text, credential leakage, or non-loopback binding.
- **Acceptance evidence:** Package tests, golden authorized definitions using exact compiled names, no-normalizer architecture checks, protocol-ID/RequestId separation and checked AgentSessionId cases, structured schema validation, stale/unknown/unauthorized-name denial, init/list/page/progress/cancel/subscription tests, and Inspector smoke over both transports.
- **Human-review triggers:** Tool name/visibility/mutation, resource URI/content, authentication/audience, redaction/text shaping, protocol baseline, or server composition.
- **Parallelism:** May overlap WP-160/WP-170/WP-180 after WP-130’s needed interfaces. It is not safely parallel with the initial WP-130 interface work.
- **Recommended PR boundary:** Adapter/catalog and fixtures; stdio via gRPC; HTTP composition; protocol/security conformance.

#### WP-160 — Durable outbox

- **Purpose:** Dispatch atomically committed event intent outside command execution with explicit at-least-once behavior.
- **Hard dependencies:** WP-100 and WP-120.
- **Upstream inputs:** Stable reciprocal event/intent records, absent-status-as-Pending semantics, explicit delivery transitions, server worker/readiness port, redaction hooks, and named failpoints.
- **Downstream interfaces:** Bounded scanner/lease loop, delivery state/backoff, idempotent Delivering normalization, derived findings/readiness, connector trait, deterministic fake connector, optional disabled HTTP connector.
- **Principal risks:** Inventing an initial status write, accepting orphan status, losing committed intent, non-idempotent restart normalization, declaring worker ready too early, implying exactly-once, hiding duplicates, command-path I/O, unbounded external data, or nondeterministic tests.
- **Acceptance evidence:** Absent/explicit Pending properties, reciprocal/orphan checks, Delivering crash/restart normalization before readiness, no-lost-event comparison, fake connector retries, process crash before/after destination acknowledgment, and visible duplicate scenario.
- **Human-review triggers:** Transaction/effect boundary, connector network/security dependency, delivery guarantee, event identity, retry clock, or stored external data.
- **Parallelism:** Safe with WP-140/WP-170/WP-180 after storage/composition interfaces.
- **Recommended PR boundary:** State/scanner; connector API/test connector; retry/duplicate/crash integration; optional HTTP separately.

#### WP-170 — Projection core

- **Purpose:** Maintain event-derived filters/counts/sums/groups under exact projection identities and non-reused generations, with atomic apply markers/frontiers and typed read-after-sequence results.
- **Hard dependencies:** WP-100 and WP-120.
- **Upstream inputs:** Checked projection plans/group schemas, `ProjectionIdentity` (lineage/ID/plan hash), ordered commits, generation-aware state/control/apply-marker store, service port, worker composition, and failpoints.
- **Downstream interfaces:** Apply engine, lifecycle/rebuild orchestration, generation-neutral one-snapshot query, frontier-fenced lower continuation, typed query/wait/status mapping, and safe notifications. It does not define storage rows or durable schema.
- **Principal risks:** Frontier ahead of state, missing/skipped marker, hash-unequal duplicate, reused/queried retired generation, in-place repair of a published generation, derived corruption weakening authoritative readiness, stale-head publication, cross-snapshot rows/frontier, unmet `after_sequence`, source dependence on derived state, or non-rebuildable values.
- **Acceptance evidence:** Model prefix/monotonicity; exact apply identity/hash; marker per sequence; initial/same-plan/changed-plan rebuild; corruption degrades the projection and rebuilds into a new generation; authoritative readiness remains intact; retired rejection; transaction-current publication; continuation invalidation; typed timeout/degraded/invalid; and crash boundaries.
- **Human-review triggers:** Identity/key/hash/generation, frontier/marker atomicity or order, lifecycle/result mapping, publication/rebuild/retirement semantics, new operator class, source-table projection, or any authoritative dependence on projection state.
- **Parallelism:** Safe with WP-140/WP-160/WP-180 after storage/composition interfaces.
- **Recommended PR boundary:** Operators/apply; frontier/query/wait; lifecycle/rebuild; process recovery properties.

#### WP-180 — Observability and diagnostics

- **Purpose:** Aggregate redaction-safe structured tracing, bounded metrics, health, and explain/operator diagnostics across existing paths.
- **Hard dependencies:** WP-120.
- **Upstream inputs:** Stable safe telemetry hooks/fields, the WP-130-injected IncidentId source, separate authoritative/derived component health ports, service plans/results, and upstream lifecycle/health hook contracts; WP-185 owns only the observability/derived-health extension wiring.
- **Downstream interfaces:** Subscribers/registry, health aggregation, diagnostic renderers, bounded label helpers, redacted incident correlation, and secret-canary tests.
- **Principal risks:** Raw data reaches a subscriber before redaction, unbounded labels, derived failures misreported as rollback, inability to instrument closed crates, or diagnostics changing semantics.
- **Acceptance evidence:** Required field/metric presence, cardinality bounds, injected IncidentId source failures and no UUID-order assumption, secret-canary/redaction integration, `last_commit_sequence = None` for an empty database, authoritative-ready plus derived-Degraded classification, and explain semantic snapshots.
- **Human-review triggers:** New raw field/label, health/readiness meaning, operator-visible error detail, sampling/export dependency, or instrumentation requiring semantic crate changes.
- **Parallelism:** Safe with other post-WP-120 packages if hooks already exist.
- **Recommended PR boundary:** Shared telemetry/health contracts; registry/subscribers; diagnostics; cross-path redaction evidence.

#### WP-185 — Server composition

- **Purpose:** Extend the runnable WP-130 `riffdbd` graph with loopback MCP HTTP, outbox/projection workers, observability, and derived-health aggregation without replacing P1 providers, startup, lifecycle, or business semantics.
- **Hard dependencies:** WP-130, WP-140, WP-160, WP-170, and WP-180.
- **Upstream inputs:** The already-running WP-130 graph and accepted P1 restart suite; stable extension/lifecycle ports; MCP adapter; outbox/projection workers; observability; ADR-0020 verbatim command names; separate authoritative/derived findings; shutdown; and telemetry. UUID, clock, cursor, digest, initialization, structural/`ValidatedCatalogHistory` proof, bootstrap/deploy/active-lifecycle, gRPC, auth, policy, service, and redb providers remain WP-130 inputs and are reused unchanged.
- **Downstream interfaces:** P2 endpoint/worker extension graph, bounded extension startup/shutdown, authoritative-versus-derived health aggregation, and real projection public-path integration. It defines no second production provider or readiness gate.
- **Principal risks:** Rebuilding rather than extending the P1 graph, adding semantics in wiring, a second service path, replacing a WP-130 provider, revalidating or bypassing the same-session startup proof, rederiving MCP names, starting workers before authoritative readiness or required derived normalization, incorrect shutdown, audience mismatch, or health overstating derived readiness.
- **Acceptance evidence:** The full WP-130 process restart suite remains green; composition tests prove the exact provider instances and staged authoritative gate are retained; exact compiled names reach MCP registration/dispatch unchanged; authoritative readiness precedes separate derived normalization/degradation; both endpoints/workers shut down cleanly; and public projection read-after-sequence passes.
- **Human-review triggers:** Component ownership, new public listener, audience/binding change, lifecycle or health meaning, direct storage path, or any business rule in server wiring.
- **Parallelism:** None among its direct inputs; it begins after their stable integration ports merge.
- **Recommended PR boundary:** One narrow P2 extension PR; semantic or P1-provider fixes go back to owning packages rather than expanding WP-185.

#### WP-190 — Integrated crash harness

- **Purpose:** Kill and restart real child processes at named authoritative boundaries and compare durable state with the reference model.
- **Hard dependencies:** WP-070, WP-100, WP-160, WP-170, and WP-185.
- **Upstream inputs:** Named failpoints already present, deterministic barriers/control protocol, temporary server lifecycle, durable inspector, and reference model.
- **Downstream interfaces:** Child-process controller, failpoint scenario DSL/config, state inspector, recovery report, and repeat-open harness.
- **Principal risks:** Replacing process death with recoverable errors, timing via sleeps, omitting pre-sequence reservation/envelope-check boundaries, inspecting through repair, skipping validation after graceful close, accidentally persisting deferred metadata, conflating authoritative and derived findings, missing UUID/database/provenance/event-hash dimensions, nondeterministic failpoints, or a path that mutates recovery state.
- **Acceptance evidence:** Exact pre/post state at every SPEC failpoint across initialization/DatabaseId; capacity reservation, sequence assignment, canonical-envelope verification, staging, and durability; sequence metadata; entities/indexes/outcomes/records; exact `EventHash` recomputation and event-intent reciprocity; provenance generation/collision/replay; capability/audit links and ADR-0021 target order; idempotency provider readiness; outbox Delivering normalization; projection degradation/new generation; and aggregate health. A reservation or one-byte-over envelope failure exposes no sequence or partial graph; graceful and crash reopen both run complete authoritative integrity; repeated restart creates no ADR-0019 deferred record and remains stable with no authoritative repair.
- **Human-review triggers:** Any third recovered state, mismatch with redb guarantees, sequence/event duplication, frontier skip, integrity repair ambiguity, or missing upstream hook.
- **Parallelism:** No useful production-package parallelism after WP-185; reporting preparation may overlap without editing the harness inputs.
- **Recommended PR boundary:** Failpoint/controller protocol; durable inspector/model; matrix cases; machine-readable report.

#### WP-200 — POC acceptance and release

- **Purpose:** Assemble the self-verifying budget demo, requirement report, reproducible benchmarks, threat/dependency evidence, and release artifacts used for the POC decision.
- **Hard dependencies:** WP-050, WP-075, WP-130, WP-135, WP-140, WP-150, WP-160, WP-170, WP-180, WP-185, and WP-190; these transitively cover the remaining packages.
- **Upstream inputs:** Stable binaries/protocols/formats, complete automated evidence, benchmark harnesses including the resolved Fjall experiment, known limitations, and security posture.
- **Downstream interfaces:** POC-001..010 JSON report, demo, benchmark report, SBOM/checksums, compatibility statement, release bundle, and architecture-review packet.
- **Principal risks:** Becoming an out-of-scope integration repair package, missing requirement ownership, irreproducible benchmarks, overstated durability/security claims, or release generation that changes tracked files.
- **Acceptance evidence:** Exact YAML commands, clean `ci-all`, asserted demo through every transport, UUID/source-owner and audit/recovery matrices, end-to-end current-validation/affected-epoch/reservation/sequence/envelope/staging evidence, exact EventHash golden-to-restart evidence, final ADR-0019 retained/deferred metadata inventory, end-to-end ADR-0020 compiler/catalog/service/MCP name evidence, verified release artifacts/checksums/SBOM, and human sign-off for all ten POC criteria.
- **Human-review triggers:** Any implementation fix outside allowed release paths, omitted criterion, benchmark engine decision, security claim, format claim, or POC/MVP scope change.
- **Parallelism:** None; this is the final integration and evidence package.
- **Recommended PR boundary:** Acceptance/demo/report; benchmarks/threat/dependency documents; release packaging; then a separate human architecture decision, not an agent acceptance.

#### P2 / POC exit evidence

P2 proves that both MCP transports expose the same policy-filtered service, a stale or unauthorized tool cannot bypass policy, an uncertain outcome resolves without a second identity/effect, outbox recovery loses no intent, and a projection query after the winning commit cannot return a prefix missing that commit. Derived degradation must not rewrite or falsely fail authoritative state. POC exit additionally needs every POC criterion, recovery boundary, reproducible generator, dependency/security report, and the resolved storage comparison.

### 6.5 P3 — Safe Symbolic Application Platform

WP-205 through WP-275 establish the application-path performance baseline,
formal RiffQL syntax, symbolic resolution, deterministic planning, one-snapshot
composite execution, textual CLI/MCP surfaces, immutable named query modules,
local development workflow, and the bounded dependent-key batch needed for the
complete TicketDesk page. Those packages remove numeric IDs, encoded key
parsing, public N+1 execution, and most hand-built authorization material from
normal application code.

ADR-0055 adds the remaining product boundary: ordinary application credentials
must be unable to fall back to storage-shaped reads, unreviewed ad-hoc queries,
undeclared referential conventions, racy application-level uniqueness checks, or
per-step budget amplification.

#### WP-280 — Closed application-query authority and private proofs

- **Purpose:** Split stable named-query, scoped ad-hoc-query, and raw-kernel
  permissions and authorize one complete resolved query rather than reusable
  storage operations.
- **Hard dependency:** WP-275.
- **Key evidence:** Exact module-hash/query-name authorization, wrong-plan and
  wrong-partition denial, non-cloneable process-local proof consumption,
  revocation races, and architecture checks preventing proof serialization or
  conversion into `GetEntity`/`ScanIndex`.
- **Compatibility:** Keep existing kernel RPC bytes and semantics; issue new
  application/agent capabilities rather than widening old permissions.

#### WP-285 — Whole-query cost authorization and execution fuel

- **Purpose:** Bind one canonical cost vector into the plan hash, compare the
  complete vector once in policy, and consume matching fuel during execution.
- **Hard dependency:** WP-280.
- **Key evidence:** Exact-budget and one-over boundaries, repeated-step
  amplification rejection, backend accounting faults, and no partial
  result/cursor after fuel exhaustion.

#### WP-290 — Declared same-partition relationships

- **Purpose:** Make required relationships contract symbols and reject commands
  that can establish or change one without a visible dominating exact target
  read, declared missing-target outcome, ordinary dependency, and commit
  revalidation.
- **Hard dependency:** WP-275. It may proceed in parallel with WP-280.
- **Key evidence:** Exact grammar/IR fixtures, same-partition complete-key type
  checks, compiler dataflow tests, explain visibility, and TicketDesk
  dangling-reference negative cases.
- **Scope boundary:** No hidden reads, optional/cascading/polymorphic
  relationships, or cross-partition references.

#### WP-295 — Declared same-partition uniqueness

- **Purpose:** Compile declared unique keys into exact conflict ownership and
  atomically maintain authoritative unique indexes with entity mutations.
- **Hard dependency:** WP-290.
- **Key evidence:** Equal-value races, value changes, replay, crash/recovery,
  corruption detection, and no sequence allocation or partial state on
  collision.
- **Scope boundary:** No inferred/global, deferrable, nullable multi-row, or
  collation-dependent uniqueness.

#### WP-300 — Safe application cutover and negative acceptance

- **Purpose:** Make named-only operations the stable SDK/MCP/dev default, keep
  agent and kernel authority on separate credentials, migrate TicketDesk, and
  machine-check the safety claim.
- **Hard dependencies:** WP-280, WP-285, WP-290, and WP-295.
- **Key evidence:** A bad-application corpus for raw kernel reads, ad-hoc
  substitution, plan/module/partition substitution, dangling references,
  uniqueness races, cumulative cost amplification, dependent-batch escape, and
  N+1/multi-snapshot page construction. Positive tests run the exact generated
  named operations through gRPC, CLI wrappers, and generated MCP tools.

P3 closes only when the stable application profile exposes named compiled
commands and exact named deployed queries, while scoped agents and kernel
operators must deliberately select separate authority. The supported safety
claim is limited to declared RiffDB semantics; undeclared business invariants,
external side effects, and multi-command workflow intent remain application
design responsibilities. `docs/safety-by-construction.md` is the concise
normative product rule and acceptance boundary.

### 6.6 P4 — Agent Application Alpha

P4 begins after WP-300. It is an application-authoring gate, not an operational
or distributed-systems gate:

> A fresh coding agent can build an unfamiliar application from an empty
> repository using only RiffDB's public textual, generated, CLI, and MCP
> surfaces.

ADR-0056 is Accepted. P4 implementation proceeds in dependency order while its
exact manifest, public/durable, generated-signature, dependency, and evaluation
fixtures retain their named review checkpoints.

#### WP-305 — Complete generated bindings and manifest

- Generate complete Rust and TypeScript named-operation clients, not only
  parameter/result types.
- Own serialization, invocation, decoding, outcomes, cursors,
  read-after-commit, errors, exact module negotiation, and safe same-key retry.
- Establish one canonical application manifest consumed by generation, roles,
  seed, scaffolding, and both languages.
- Gate on zero handwritten RiffDB transport/encoding glue.

#### WP-310 — Symbolic roles

- Compile role operation names into exact application authority and private
  query requirements.
- Derive contract description, result visibility, bounds, lineage, and MCP
  visibility without exposing field masks or kernel permissions.
- Bind by role, principal, environment, and tenant scope; make missing
  requirements actionable by symbol.

#### WP-315 — Structured application errors

- Add one bounded versioned application-error envelope while retaining
  compatible kernel error bytes.
- Preserve authorized operation, contract/module, symbol/span, required
  permission/resource, closed remediation, retry/recovery, and trace context.
- Prove semantic parity and redaction across gRPC, Rust, TypeScript, CLI, MCP,
  logs, and traces.

#### WP-320 — Command batches and seed/import

- Run bounded concurrent streams of ordinary exact named commands.
- Keep per-item idempotency, authorization, typed outcomes, provenance, and
  commits; never claim one batch transaction.
- Add backpressure, progress, cancellation, checksummed resume, crash replay,
  and `riffdb dev --seed` integration.

#### WP-325 — Scaffold and unavoidable application boundary

- Make `riffdb new <application>` followed by `riffdb dev` the canonical path.
- Generate the manifest, contract, queries, roles, seed, protected credential
  configuration, MCP setup, and Rust/TypeScript output roots.
- Default application facades exclude kernel APIs; explicit admin/unstable
  packages or features and separate credentials retain low-level access.
- Make dependency/source boundary linting mandatory.

#### WP-330 — TypeScript runtime parity

- Ship a real TypeScript web application, not only compiling generated source.
- Consume the same manifest, operation schemas, error fixtures, and golden
  observations as Rust.
- Exercise queries, commands, outcomes, pagination, nested/optional results,
  read-after-commit, errors, credentials, reload, and MCP-assisted discovery.

#### WP-335 — Two-domain evidence and bounded RiffQL closure

- Build blog/CMS and orders/inventory corpora through both language paths.
- Preserve every unsupported shape and its actionable diagnostic.
- Classify whether an index, declared relation, projection, or decomposition
  already solves the need.
- Stop for a separate exact accepted ADR before adding any grammar, IR, plan,
  authorization, cursor, storage, or result semantic; implement only the
  smallest repeated-use bounded construct after that review.

#### WP-340 — Sealed independent evaluation

- Run four isolated fresh-agent builds: both domains in both Rust and
  TypeScript.
- Provide public binaries, docs, builder MCP, and generated packages, but no
  RiffDB implementation or TicketDesk source.
- Publish raw measurements for interventions, kernel attempts, handwritten
  glue, failures per feature, query gaps, first-write/read/completion time,
  source access, and rating.
- Require zero workaround intervention, successful kernel use, kernel imports,
  handwritten glue, prohibited source access, or unresolved required query
  shape; first write within 30 minutes, first page within 60 minutes, and every
  rating at least 8.5/10.

The detailed gate is in `docs/agent-application-alpha.md`. Failures return to
the owning package; evaluation applications cannot patch around product gaps.

### 6.7 Roadmap Toward MVP

- **Agent Application Alpha:** Complete WP-305 through WP-370, including the
  compiler-owned application lock, authoring recovery, growing-database
  performance gate, sealed rehearsals, canaries, and campaign 02 before
  operational hardening or replication.
- **Stage A, single-node alpha:** Add a real migration framework, stable format policy, online consistent backup/verified restore, bundle signing, bounded indexed reads, approved repair operations, remote TLS/OAuth MCP, production-hardened TypeScript compatibility plus generated Python, quotas, projection/backfill controls, and upgrade/downgrade compatibility. Gate on a trusted design-partner workload with documented recovery and incident procedures.
- **Stage B, replicated beta:** Put deterministic normalized commit application behind a replication facade, add snapshots/membership/catch-up/leader routing, and prove idempotency/outcomes unchanged through leader loss and network partitions. Do not add Raft to the POC path.
- **Stage C, partitioned MVP:** Add tenant-local leaders, placement epochs, fenced movement, production authorization/audit/rate limits, stable clients, operational projections, agent branches/replay, CDC/export, and full operational support. Continue rejecting undeclared cross-partition mutations.
- **Post-MVP research:** Distributed transaction/saga semantics, escrow/commutative types, global uniqueness/indexes, multi-region placement, richer incremental analytics, and verified migrations remain explicit research tracks. They must not leak into POC abstractions beyond preserving deterministic, replication-shaped records.

The budget comparison application remains a compatibility and evidence canary across these stages. New guarantee profiles are added only when both backends implement the claimed observable semantics; historical workload fixtures remain runnable so API ergonomics, implementation complexity, correctness, and performance can be compared over time.

### 6.8 P7 — Robust Offline Contract Migration

P7 is planned by WP-405 through WP-413 and remains unavailable until
ADR-0076 through ADR-0079 are accepted exactly. The dependency graph is
`diagrams/migration_work_package_dag.dot`.

- `.riffm` source compiles into one canonical migration bundle bound to exact
  parent and successor bundle hashes. Application source V3/lock V4 may retain
  at most 32 direct parent-specific migration artifacts.
- Migration expressions are deterministic and row-local. Lossless or asserted
  conversions are supported; SQL, callbacks, I/O, time, randomness, lookups,
  general scans, skip-row behavior, and generic administration writes are not.
- Apply drains one selected database, rejects retiring-version Pending
  admissions, creates an immutable automatic backup, transforms a private
  same-filesystem staged copy in journaled bounded batches, rebuilds projections,
  validates completely, and atomically publishes the stage.
- A commit-owned MigrationCoordinator remains the sole authoritative mutator.
  Migration changes no application sequence and rewrites no commit, event,
  outcome, provenance, idempotency, or historical bundle bytes.
- Cutover retires predecessor writes. Post-publication validation failure
  automatically restores the operation backup before readiness.
- Only a dedicated lineage-scoped migration capability may use the three gRPC,
  Rust SDK, and CLI operations. MCP and application drivers expose no migration
  path.

Delivery is gated: WP-410 proves additive data/constraint changes, WP-411 adds
renames/logical retirement/type replacement, WP-412 adds keys/partition/
aggregate/conflict changes, and WP-413 closes installed, recovery, handbook,
PostgreSQL-comparison, and performance evidence.

### 6.9 P8 — Reactive Applications

P8 is owned by WP-414 through WP-421 under accepted ADR-0080. It extends the
existing authoritative event and named-query model rather than adding a second
broker, raw CDC surface, or event-sourcing requirement.

- Contract events may opt into application streaming with compiler-proved
  partition fields. A generic integrity-checked route index provides bounded
  partition-order replay while catalog remains the sole historical payload
  materialization authority.
- Reactive application modules define explicit event streams and contextual
  subscriptions. Application Source V4 and Lock V5 follow the migration-owned
  V3/V4 formats, and roles independently name streams, watched queries, and
  contextual subscriptions.
- Durable consumers provide bounded at-least-once delivery, leases, contiguous
  checkpoints, ack/nack, retry, dead-letter, seek, recovery, and payload-free MCP
  wakeups. They do not claim exactly-once external effects.
- Live named RiffQL begins with a one-snapshot result at frontier S and catches
  up authoritative commits after S. Compiler-proved public keys permit bounded
  patches; every ambiguous or excessive case resets to a complete result.
- Contextual work rehydrates all named queries in one freshly authorized
  snapshot at or after the event, filters declared commands by current
  capability, and composes lease-bound causation with generated deterministic
  reaction idempotency.
- Rust, TypeScript, Python, CLI, and MCP share one API-neutral model. Browsers
  connect through generated application-owned SSE relay code and never receive
  database capabilities.

RT-1 through RT-5 close in WP-421 with TicketDesk browser, worker, and agent
acceptance. Connectors, declarative reactions, cross-partition streams, physical
event retention, historical entity snapshots, and in-database inference remain
explicit later work.

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
| `scripts/` | WP-000 plus exact package patterns | WP-020 establishes `generate-proto*`; the exact WP-010 error slice consumes it without changing the script; WP-065 and WP-127 extend it only for their durable/remaining-public schema phases; WP-040 owns `generate-contract-fixtures*`; WP-070 `storage_*`; WP-140 `mcp-inspector-smoke`; WP-150/WP-200 `demo*`; WP-190 `recovery_*`; WP-200 `release*` |
| `.github/` | WP-000; WP-045 owns only `workflows/budget-comparison.yml` | Core CI/PR templates stay with WP-000; the isolated comparison workflow follows its evidence package |
| `proto/`, `fixtures/proto/` | WP-020 baseline; exact WP-010 execution-failure error slice; WP-065 durable completion; WP-127 remaining public completion | Real phased schema and compatibility artifacts only. The WP-010 carve-out adds no request/result/service/RPC/durable fields; WP-065 owns durable messages/codecs; WP-127 owns every remaining public wire structure; WP-130 owns neither and implements conversions in its adapter crate |
| `contracts/parser-fixtures/` | WP-030 | Selected grammar corpus |
| `fixtures/compiler/`, `contracts/examples/budget.riff` | WP-040 | Golden plans/schemas/diagnostics and canonical budget source |
| `fuzz/Cargo.toml`, `fuzz/fuzz_targets/contract_*` | WP-030 | Parser fuzz workspace/targets; later targets need a declared owner |
| `tests/contract_deploy*`, `tests/storage_recovery/**`, `tests/service_audit_recovery/**`, `tests/command_semantics/**`, `tests/service_audit/**` | WP-050, WP-070, WP-070, WP-100, WP-100 respectively | Catalog, storage/audit recovery, command semantic, and service-audit ordering targets |
| `tests/authorization/**`, `tests/service/**`, `tests/grpc/**`, `tests/mcp/**` | WP-110, WP-120, WP-130, WP-140 respectively | Security, service, and protocol integration targets |
| `tests/outbox/**`, `tests/projection/**`, `tests/observability/**`, `tests/server_composition/**`, `tests/recovery/**` | WP-160, WP-170, WP-180, WP-185, WP-190 respectively | Derived-system, final-wiring, and recovery integration targets |
| `benchmarks/storage-fjall/**` | WP-075 | Isolated non-production Fjall workspace, adapter, suite, and evidence |
| `benchmarks/**` | WP-200 except the WP-075 subtree | Reproducible process scenarios/reports; component benches stay in owning crates |
| `examples/budget-comparison/**` | WP-045, WP-125, WP-135 | Isolated workload/PostgreSQL, service adapter, then public SDK/gRPC adapter under exact subpaths |
| `examples/**` | WP-150/WP-200 | Public API/demo packaging; preserve comparison-workstream ownership |
| `docs/` | WP-200 | Generated/reference docs and security/compatibility statements |

Do not add meaningless marker files merely to force empty directories into Git. `PLAN.md` records the topology until real artifacts exist.

The root is a virtual workspace, so each top-level integration test above is an
explicit `[[test]]` target in exactly one owning crate manifest, with a
package-qualified acceptance command. A package may point that target at
`../../tests/...`; it must not create a generic root test crate or register the
same test from multiple manifests.

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

ADR-0001 through ADR-0007 and ADR-0009 through ADR-0021 are frozen inputs, not
open choices. In particular, ADR-0012 owns deterministic logical time,
no-command-randomness, and terminal non-commit execution-fault disposition;
ADR-0017 is **Projection Group Keys, Generations, and Durable Frontiers**, not a runtime
fault placeholder. ADR-0018 owns pure UUIDv7 construction, the exact production
source graph, durable DatabaseId installation, provenance generation/replay, and
request/capability identity boundaries. ADR-0019 freezes the six-category POC
metadata surface and unconditional startup integrity pass without a `NodeId`,
shutdown marker, or persisted integrity history. ADR-0020 freezes the
compiler-owned command tool-name registry and catalog revalidation. ADR-0021
freezes the nine lineage-scoped service-audit targets, canonical list, and
shared-service-only derivation. The accepted batch also authorizes formal WP-065
and WP-127, the reviewed redb/capability
dependency baselines, and the exact
`EvaluatedCommand`/`CommitIntent`, policy verifier/clock, audit, and projection
boundaries reflected above.
The 2026-07-14 accepted ADR-0004/ADR-0011 amendments also freeze the exact
pre-sequence candidate/capacity chain and the durable-event hash preimage. They
are implementation inputs, not additions to the open ADR queue.

The smallest remaining POC architecture queue contains only ADR-0008. Exact-text
acceptance still requires explicit human approval.

### ADR-0008 — Remaining Native MCP Resource and Transport Model

- **Why:** Resource URIs, schema presentation, configured audience, stdio-via-gRPC, HTTP composition, cursor presentation, pagination, and text safety remain public compatibility/security boundaries. ADR-0020 already owns command names and normalization.
- **Blocks:** WP-180 and WP-140 directly; WP-185 and WP-200 transitively.
- **Options:** Percent-encoded typed IDs vs stable numeric URI segments; exact canonical URI grammar; stdio gRPC vs in-process; configured HTTP audience; cursor and result-text encoding; list-change/subscription behavior.
- **Recommended proposal:** Stable canonically encoded resource URIs; stdio uses public gRPC; HTTP uses one configured canonical audience; transport presentation remains bounded and redacted; every invocation reauthorizes through the shared service.
- **Decision point:** Develop after ADR-0007/service interface; accept before WP-180 or WP-140 starts, whichever is earlier.
- **Development mode:** May be completed after P1 service interfaces; it must be accepted before MCP-aware observability fixtures or MCP implementation.

## 9. Verification Matrix

| Guarantee | Techniques and required assertions | Earliest package | Final end-to-end evidence |
|---|---|---|---|
| Safe Rust, reproducible workspace, dependency policy | Crate-root `forbid`, fmt/Clippy/test/doc/deny, package inventory, clean generated diff; separately inventory transitive unsafe/native code | WP-000 | WP-200 `ci-all` and release verification |
| Canonical IDs/values/decimal/keys/hashes | Unit tests, Proptest ordering/overflow/scale algebra, cross-platform golden bytes/hashes, encoder/decoder fuzz | WP-010 | WP-100 identity/history tests and WP-200 demo |
| UUIDv7 construction, source isolation, and replay | Exact byte/text/max/overflow goldens; wrong version/variant/length tests; dependency-owner and no-ambient-source checks; fake source calls; database install/reopen, provenance collision/unknown/replay, fresh RequestId, retained bootstrap ID/token, and normal-create token-unavailable cases | WP-010 | WP-110/WP-130 providers, WP-190 crash matrix, and WP-200 |
| Public-safe errors and redaction | Unit/property bounds, safe conversion snapshots, secret canaries through logs/gRPC/MCP, business outcome not transport error | WP-010 | WP-180 telemetry redaction and WP-200 |
| Phase-zero, durable, and public Protobuf compatibility | WP-020 common/envelope/service-descriptor goldens; exact WP-010 execution-failure error descriptor/mapping/preflight/wire goldens; WP-065 durable descriptors/records/storage-codec round trips plus conservative complete-envelope upper-bound proofs and equal/one-byte-over fixtures; WP-127 remaining public descriptors/schema hashes/wire goldens; reserved-field tests, decoder fuzz, deterministic regeneration, and dependency-direction checks | WP-010 for the amended current boundary | WP-065/WP-127 freezes, WP-070 actual-envelope enforcement, WP-190 recovery, and WP-200 release |
| Parser bounds and diagnostics | Valid/invalid corpus, source-span snapshots plus semantic assertions, parser/round-trip fuzz with production limits | WP-030 | WP-040 fixture and WP-200 demo compile |
| Deterministic IR/plan/schema and MCP command names | Golden bundle/plan hash/JSON Schema, repeated-build equality, compatibility properties, unsupported IR rejection, and negative proof that POC grammar/AST/HIR/IR/bundles contain no contract state-machine surface; exact ADR-0020 case mapping, segment/128-byte boundaries, collision spans, CommandId ordering, registry/hash fixtures, and WP-050 activation revalidation | WP-040 | WP-050 catalog, WP-120/WP-140 verbatim consumption, and WP-200 |
| Runtime determinism and invariant preservation | Pure predicate/invariant/postcondition/commit-check unit tests, property-generated command histories, same snapshot/input/time `EvaluatedCommand` comparison, single-thread reference-model differential testing, and architecture checks excluding state-machine dispatch, admission/provenance/I/O | WP-080 | WP-100 intent assembly/real concurrency and WP-200 POC-002/003 |
| Deterministic execution-fault disposition | Fixed pre-admission arithmetic vectors mapped to root `OutOfRange` with no pending state; fixed post-snapshot arithmetic/resource-fault vectors; assert no evaluated command/sequence/provenance/commit; mutate each influential dependency before terminalization; failpoints distinguish proven abort from unknown commit; public error descriptor/mapping fixtures | WP-060/WP-080, then WP-120 for the pre-admission service boundary | WP-100/WP-127/WP-130, WP-190, and WP-200 |
| Complete read/predicate and affected-epoch tracking | Compiler negative tests, runtime evidence assertions, mutate each influential read before commit, safe rejection of undeclared dependencies; prove influential range dependencies and mutation-affected prefix targets may overlap or differ and cannot substitute for each other | WP-040/WP-060/WP-080 | WP-100 concurrency/revalidation and WP-190 |
| Snapshot/storage semantic conformance | One parameterized suite over memory and redb: absence/version, bounded scan/epoch, ordered scan, atomic state/model equality | WP-060 | WP-070 and WP-190 |
| Staged startup proof and first runnable process | Memory/redb initialization-first structural-session type-state; bounded page/continuation/exact-end properties; catalog historical IR resolver with skipped/repeated/truncated/cross-session rejection; architecture checks excluding storage-to-IR, generic callbacks, and proof-through-storage; matching dormant-port/proof join; digest inventory, lifecycle, clean-stop/reopen, and retained bootstrap-replay assertions | WP-050/WP-060 | WP-130 real temporary-redb gRPC restart checkpoint, then WP-190/WP-200 |
| Conflict exclusivity/fairness/cancellation | Unit tests; Loom reduced grant/release/cancel/timeout/fairness/lost-wakeup model; Shuttle multi-key schedules; barriers/hooks, no sleeps | WP-090 | WP-100 budget race and WP-200 |
| Pre-sequence capacity and atomic sequence/mutations/outcome/events/provenance/log | Type-state/compile checks and reference properties for count-only start, current validation, affected-epoch read, sequence-free plan, semantic/conservative reservation, assignment, retained-candidate verification, actual canonical envelope charge, and staging; one full outcome envelope replaces pending atomically with no pointer/tombstone/second terminal record; WP-060 synthetic equal/over-charge cases; WP-065 real-codec equal-bound/one-byte-over fixtures; failpoints at every boundary; inspect the entire record set and sequence visibility | WP-060 memory, then WP-065 codec proof without a back-edge | WP-070 durable enforcement, WP-100 semantics, WP-190 matrix, and WP-200 |
| Exact durable `EventHash` | Golden full domain frame over 12-byte EventId, EventTypeId u32 BE, payload length u32 BE, and canonical record bytes; field-order/ordinal/type/length/cross-domain mutations; storage-API constructor rejection; Proto round trip; startup reciprocal graph mismatch and crash/reopen rejection without repair | WP-060 semantic constructor and WP-065 wire/goldens | WP-070 integrity, WP-100 assigned graph, WP-190 crash matrix, and WP-200 |
| Idempotent uncertain-response recovery | Canonical identity/hash properties, equal/mismatch cases, one full `StoredOutcomeV1` terminal row, atomic pending deletion without tombstone, no pointer/second terminal envelope, exact startup outcome/commit/provenance reciprocity and malformed-edge rejection, pending/committed/failed digest-provider readiness scan, referenced-key retirement rejection, kill after durable commit, same sequence/result and no second mutation/event | WP-065/WP-070/WP-100 | WP-190 and WP-200 POC-004/005 |
| Catalog activation/compatibility | Expected-version races, compatibility golden fixtures, process crash before/after pointer, notification timing | WP-050 | WP-140 list changes, WP-190/WP-200 |
| Authorization non-bypass and obligations | Deny-default matrix, revocation/environment/audience/tenant/field tests, initial and fresh-final facts around bounded queue wait, one transaction-current capability timestamp across verifier/state/audit, architecture checks excluding storage from policy and predicate duplication from commit, stale MCP invocation, secret canaries | WP-110 | WP-120 service, WP-130 gRPC, WP-140 MCP, WP-200 POC-008 |
| Durable service and bootstrap audit | WP-010 target tag/key/permutation/duplicate/bound goldens; memory/redb conformance and malformed/order recovery; exhaustive 22-operation request-only target mapping with start/terminal equality and no result expansion; exact mutation/admin/Deny/obligated-read scope; initial-auth/Started/permit/fresh-auth/admit order; exactly one selected/attempted terminal and at most one durable terminal per Started; closed independent links; obligations before Succeeded; clock/append failure map; stream-establishment-only audit; compound bootstrap one-clock linkage; credential sync; process kill/reopen without terminal synthesis | WP-010/WP-060/WP-065 | WP-070/WP-100/WP-120/WP-150, WP-190, and WP-200 |
| API-neutral service | In-process fake/real port integration, dependency graph checks forbidding concrete storage imports, policy-fact/decision assertion per operation, audit-before-output checks, and server-side cursor fencing | WP-120 | WP-130/WP-140 transport E2E and WP-200 |
| gRPC conformance and client retry | WP-127 descriptor/schema-hash and UUID validation fixtures; WP-130 conversions, status-vs-outcome mapping, malformed/limit/deadline tests, generated signatures, fresh RequestId plus same-key safe retry/unknown resolution, and real child-process bootstrap/deploy/budget/clean-restart/durable-reread/bootstrap-replay evidence | WP-127/WP-130 | WP-130 first-runnable checkpoint and WP-200 POC-001/004 |
| MCP schema/protocol/authorization | Golden tools/resources, JSON/canonical fuzz, output-schema validation, hosted RequestId versus JSON-RPC/progress-ID separation, init/list/page/progress/cancel tests, Inspector both transports, per-tool auth | WP-140 | WP-200 POC-001/007/008 |
| Outbox atomic intent and at-least-once | Event/intent reciprocity; absent status equals never-attempted Pending; no orphan status; explicit retry metadata; state-machine properties, fake connector, Delivering restart normalization before readiness, crash before dispatch/after destination success, duplicate visible | WP-060/WP-070 | WP-160, WP-190, and WP-200 POC-005 |
| Projection identity/generation/prefix/frontier/rebuild | Identity/key/apply-hash goldens; marker per sequence; exact apply identity/hash; lifecycle model; transaction-current publication; retired rejection; one-snapshot continuation fencing; inconsistency degrades derived state without changing authoritative readiness; rebuild only into a new generation; crash tests | WP-010/WP-040/WP-060 | WP-170, WP-190, and WP-200 POC-006 |
| Recovery equivalence and operational-metadata boundary | Named process failpoints, child kill/reopen, inspector/reference comparison, DatabaseId preservation, exact sequence successor/exhaustion, event/intent reciprocity, capability/bootstrap/audit links, all idempotency key schemes readable, fail-closed authoritative mismatch with no repair, separate bounded derived findings/normalization/degradation, complete integrity validation after graceful close and crash, negative schema/key checks for ADR-0019 deferred metadata, and repeated-open idempotence | WP-060/WP-065/WP-070 | WP-190 full matrix and WP-200 POC-009 |
| PostgreSQL/RiffDB example parity | Golden workload/observation fixtures and PostgreSQL tests in WP-045; parameterized service-adapter conformance in WP-125; public SDK/gRPC conformance in WP-135; correctness preflight before every benchmark | WP-045 | WP-200 process comparison report using the same runner |
| Performance and regressions | Dependency-free repeated-run component harnesses, including WP-070 storage, plus process workloads for conflict-free/hot-key/replay/scan/restart/durability; record revision/environment/distribution | WP-070, then WP-090/100/170 | WP-200 published redb/Fjall report |
| Generated-artifact reproducibility | WP-000 registry; byte-for-byte phase-zero/WP-065 durable/WP-127 public proto, IR/schema/docs, SDK, MCP regeneration; clean tracked/untracked status | WP-000 | WP-200 release verification |

Fuzz, Loom, Shuttle, and full crash jobs run in dedicated profiles. Process correctness tests use explicit control barriers, never sleeps. Benchmarks are architecture evidence and regression signals, not an invented TPS release threshold.

## 10. Risk Register

| Risk | Trigger / early warning | Affected WPs | Consequence | Mitigation and required decision/experiment | Threat |
|---|---|---|---|---|---|
| Generated bindings remain metadata rather than a complete client | Acceptance apps contain parameter maps, result-tag switches, RPC wrappers, cursor/status parsers, or retry identity construction | WP-260/305/325/330/340 | Every application recreates transport glue and can diverge from module, error, and retry semantics | One manifest/operation registry; complete generated invocation/codec/recovery methods; zero-glue source scanner; Rust/TypeScript differential goldens | Agent Application Alpha viability |
| Symbolic roles leak reusable kernel authority | Role compilation emits `ReadEntity`/`ScanIndex`, masks, or broad ReadContract grants instead of private exact-query requirements | WP-280/300/310/340 | Application code regains N+1, multi-snapshot, field overreach, or hidden storage-shaped access | ADR-0055 proof boundary; role names operations only; sealed compiler-derived requirements; escalation and substitution tests | Core safe-application security claim |
| Rich application errors leak schema, values, or arbitrary model-facing prose | Error context is attached before authorization/redaction, includes submitted values/internal sources, or differs by transport | WP-315/330/340 | Security disclosure, prompt injection, inconsistent recovery, or operator-hostile fallback | Closed bounded fields/fix codes; service-owned redacted builder; secret/hidden-schema canaries; cross-transport semantic fixtures | Security and agent self-correction |
| Batch import becomes an alternate mutation protocol | Batch owns a transaction, bypasses commands, changes idempotency/outcomes, or retries with new keys | WP-320/325/340 | Invariants, provenance, uncertainty recovery, or authorization can be bypassed for seed/import | Per-item ordinary command path; explicit non-atomic semantics; checksummed same-key resume; crash/revocation/backpressure tests | Core command-only mutation claim |
| Agent evaluation overfits examples or hides intervention | Agents see TicketDesk/internal source, evaluators patch product gaps, only aggregate ratings are published, or one language/domain is omitted | WP-335/340 | A high score provides no evidence of general application authorship | Sealed release bundle; four isolated domain/language runs; raw event/metric reports; zero-workaround rule; owner-routed failures | Product evidence integrity |
| Application authority collapses into kernel permissions | A stable application capability can call `GetEntity`/`ScanIndex`, submit ad-hoc source, or substitute a different named module/query/plan | WP-250/260/280/300 | Application code can recreate N+1, multi-snapshot, overbroad-read, and unreviewed-query bugs despite RiffQL | ADR-0055; disjoint exact permissions; one whole-query policy request; private plan-bound proof; named-only generated clients; negative capability/substitution tests | Core safe-application product claim |
| Query budgets amplify across individually valid steps | Policy compares each scan/batch with the same ceiling or the backend performs work not charged to the admitted plan | WP-230/240/280/285/300 | A bounded-looking query exceeds capability resource limits or returns partial data after exhaustion | Canonical plan-hashed cost vector; one aggregate policy comparison; decrementing execution fuel; exact/one-over and adversarial accounting tests | Availability and tenant-isolation boundary |
| Declared integrity can be omitted or races under concurrency | Relationships remain scalar conventions, uniqueness is checked by a prior query, or enforcement injects hidden reads outside the plan | WP-290/295/300 | Dangling references, duplicate business keys, invisible dependencies, or write skew reappear in normal application code | Explicit same-partition reference/unique declarations; compiler-visible exact dependencies/conflict keys; commit revalidation and atomic unique-index maintenance; concurrent/crash negative tests | Core safe-by-construction claim for declared semantics |
| Contract language scope growth | Requests for loops, callbacks, arbitrary scans/I/O, both conflicting syntaxes, or runtime AST access | WP-030/040/080 | Static read/effect/conflict visibility and termination no longer hold | Select one grammar; reject unsupported constructs; require semantic corpus/model evidence and human review for every construct | Core concept if static visibility is lost |
| Hidden nondeterminism | Runtime imports clocks/random/env/filesystem, unordered map serialization, variable bundle metadata, or schedule-dependent output | WP-010/040/080/100 | Replay/model/replication-shaped semantics diverge | ADR-0011/0012; pure value context; canonical collections; determinism fixtures and differential histories | Core concept |
| UUID source ownership or replay drifts | A fourth direct entropy owner appears, types/runtime/storage access ambient sources, a retry regenerates durable identity, UUID order substitutes for sequence order, or provenance collision occurs after allocation | WP-010/060/070/100/110/130/150/185/190 | Nondeterminism, duplicate/ambiguous durable records, unsafe uncertainty recovery, or dependency sprawl | ADR-0018; pure assembler; auth/client/server-only dependency check; injected ports; exact call-count/collision/replay/crash tests; sequences remain sole order | Core uncertainty proof if identity duplicates; otherwise implementation/security |
| Storage transaction held across execution | Live redb transaction or guard appears in runtime context/intent; long writer stalls | WP-060/070/080/100 | Throughput collapse, cancellation hazards, engine leakage, unclear atomicity | ADR-0004 prototype comparing bounded snapshot plus coordinator transaction; compile-time ownership tests | Selected implementation, unless needed for correctness |
| Incomplete read dependency tracking | Runtime reads absent from plan/evidence; invariant fails only under interleaving | WP-040/060/080/100 | Invalid committed state | Compiler fail-closed analysis, materialized evidence for every read, mutate-before-commit adversarial tests, reference model | Core concept |
| Influential and mutation-affected epochs are conflated | Affected advances are inferred from snapshot dependencies, influential ranges are omitted because no mutation touches them, or one collection type is reused | WP-040/060/070/080/100/190 | Phantom/read validation can be incomplete or required prefix epochs fail to advance | ADR-0004 distinct checked collections; derive affected targets from exact old/new entries after validation; overlap/difference conformance and crash tests | Core invariant proof if influential reads escape; otherwise durable implementation |
| Predicate revalidation cannot use exact plan | Only invariant ID/captured values survive, catalog lacks historical plan, plan hash mismatch | WP-040/050/060/080/100 | Commit validates the wrong rule or cannot validate | ADR-0003 chooses embedded validated commit-check representation or historical immutable lookup; version/hash assertions | Core concept |
| Sequence assigned before exact capacity | Capacity is guessed at candidate start, transaction-current envelope charge appears only after allocation, one-byte-over fails after allocator advance, or actual codec bytes exceed the reserved class | WP-060/065/070/100/190 | Invisible gaps, late partial staging risk, or a batch exceeds its hard bound | ADR-0004 count-only start and sequence-free plan; WP-065 conservative real-codec proofs; WP-070 actual-byte check before staging; type-state and boundary failpoints | Core contiguous-sequence/atomicity proof |
| Lock cancellation/fairness defects | Lost wakeup, queue growth, double grant, starved multi-key waiter, leaked lease after cancellation/panic | WP-090/100 | Deadlock, availability failure, or conflicting execution | Reduced Loom model, Shuttle schedules, RAII lease, explicit barriers, bounded queues and wait metrics | Selected implementation; double grant threatens concept proof |
| Idempotency fails after uncertain response | Duplicate event/mutation, different result/sequence, lost `tx.time`, orphan reservation, or a pending/terminal identity whose digest key is no longer readable | WP-060/070/100/130/140/190 | POC’s uncertainty-recovery thesis fails | ADR-0005; atomic terminal set; reservation crash model; startup scan of all three identity states; referenced-key retirement rejection; kill after commit and same-key retry | Core concept |
| Durable format evolves unsafely | Field/key reuse, fixture drift, decode/re-encode loses required data, table examples disagree, or WP-070 persists a DTO not frozen by WP-065 | WP-020/050/060/065/070/100/170 | Startup failure, silent reinterpretation, unrecoverable data | ADR-0006; WP-065 owner gate; reserved fields, versioned envelopes/keys, old/current fixtures, offline idempotent migration policy | Implementation now; product viability if unmanaged |
| Event hash identity drifts | One layer hashes payload-only/Protobuf bytes, omits EventId/type/length, changes byte order, or accepts a stored mismatch | WP-010/060/065/070/100/160/170/190/200 | Reciprocal event/outbox evidence can alias or fail across restart and format evolution | ADR-0011 exact preimage/domain; one storage-API builder/constructor check; WP-065 framed goldens; WP-070 recomputation; crash/recovery mismatch rejection | Durable integrity and idempotency evidence; selected hash design is replaceable only by migration |
| Deferred operational metadata leaks into the POC | A `NodeId`, shutdown marker, persisted integrity timestamp, placeholder key/field, or startup fast path appears | WP-060/065/070/130/185/190/200 | Premature durable compatibility surface or skipped integrity validation | ADR-0019; retained-set conformance and schema negative fixtures; complete validation after graceful/crash restart; no marker writes | Selected implementation and future migration risk |
| Startup proof ownership collapses or can be bypassed | Redb imports IR/catalog, `ValidatedCatalogHistory` crosses storage, a callback runs inside the engine, a truncated scan counts as end, dormant ports activate without a matching session, or WP-185 rebuilds the gate | WP-050/060/070/130/185/190 | Cyclic dependencies, unvalidated historical plans, interleaved mutation during evidence, or a process serving from structurally/semantically inconsistent state | ADR-0004 split proof; exact-end/session-bound types; opaque catalog-owned `ValidatedCatalogHistory`; WP-130-only join; compile-time dependency checks; memory/redb/catalog adversarial cases; real graceful/restart process evidence | Core authoritative-readiness boundary |
| MCP authorization bypass | Adapter imports storage, visibility substitutes for auth, stale hidden tool succeeds, obligations applied after rendering | WP-110/120/140/180/200 | Unauthorized data access/mutation and failed thesis | Dependency architecture test, service-only adapter, invoke-time reauthorization, stale-name and secret-canary tests | Core concept/security |
| MCP command identity is rederived or repaired | Service/adapter lowercases independently, catalog accepts a malformed registry, names are suffixed/truncated, or a case-only collision reaches activation | WP-040/050/120/140/185/200 | Public names drift across compilation, discovery, and invocation | ADR-0020; compiler goldens/properties, catalog revalidation, verbatim-consumption architecture tests, stale-name denial | Public compatibility and security-adjacent implementation risk |
| Projection identity/generation/frontier is incorrect | Identity aliases a changed plan, generation is reused or repaired in place, frontier exceeds rows/markers, a sequence is skipped, retired rows become queryable, publication uses a stale head, pagination joins snapshots, or derived damage changes authoritative readiness | WP-010/040/060/065/070/127/170/185/190/200 | Derived consistency and rebuild claims are false or the authoritative database is unnecessarily unavailable | ADR-0010/0017; exact identity/key/hash fixtures, marker per sequence, atomic state/marker/frontier, degrade then rebuild into a new generation, retired rejection, transaction-current publication, one-snapshot queries, separate health and crash tests | Core POC proof; engine is replaceable |
| Excessive crate fragmentation | One-line forwarding crates, cyclic dependencies, duplicate DTOs, frequent cross-crate changes | WP-000 onward | Slow integration and accidental public interfaces | Keep 29 specified crates but no fake APIs; owner map; package-private types where possible; combine PRs by semantic boundary, not new crates | Selected implementation |
| Agents change shared types concurrently | Parallel PRs edit `riffdb-types`, IR, storage API, proto, policy facts, service DTOs, or fixtures | All waves | Incompatible assumptions, duplicated types, unstable durable/public contracts | Interface-first PRs, named owner, merge revision in prompts, WP-065 durable/WP-127 public schema ownership, coordinated rebase, stop on out-of-scope interface need | Delivery risk |
| Proto phase ownership is bypassed | A durable field is added outside WP-065, a public field outside WP-127 other than WP-010's exact ADR-0012 error slice, Proto depends on storage, or WP-130 invents wire semantics during conversion | WP-010/020/060/065/070/120/127/130/160/170 | Cyclic dependencies, accidental compatibility break, or generated wire types become semantic APIs | Enforce the exact WP-010 exception and later phase owners plus one-way storage-codec-to-Proto dependency; schema-hash/golden review before persistence/adaptation; architecture checks; reserve only justified fields | Selected implementation |
| Transaction-current authorization is stale or duplicated | Queue delay crosses expiry/revocation, commit copies only part of a capability record, commit reimplements policy predicates, or policy opens storage | WP-070/100/110/120 | A previously allowed request mutates under revoked/expired authority or two paths disagree | Policy-owned fresh `AuthorizationClock`, complete mechanical record-to-facts lowering, pure verifier inside the coordinator transaction, differential fact/verifier schedules, dependency bans | Core security boundary |
| Durable invocation audit or admission is incomplete | Required mutation/admin/Deny/obligated-read scope is missed, Started is absent/duplicated, capacity wait occurs before Started, final authorization is stale, the terminal phase is not selected/attempted once, cancellation hides uncertainty, output precedes terminal durability, clock/append outage is ignored, or bootstrap linkage tears | WP-060/065/070/100/120/130/150/180/185/190 | MCP-046 and forensic provenance claims fail; protected actions can be unaccounted or returned under stale authority | Closed scope/phase/link/failure map; initial auth then Started/permit/fresh auth; fail-closed clock/append behavior; obligations before Succeeded; crash-safe compound bootstrap; deterministic schedules and process recovery | Core security/evidence boundary |
| Service-audit targets drift or leak result data | A bare lineage-local ID is persisted, storage/adapter derives targets, list order varies, a result link/returned row is copied, or start/terminal targets differ | WP-010/060/065/070/100/120/190/200 | Audit identity becomes ambiguous, unbounded, transport-dependent, or disclosure-prone | ADR-0021; one canonical tag/payload key and bounded list; service-only exhaustive mapping; durable malformed/order rejection; start/terminal equality and no-expansion tests | Security/evidence and durable compatibility boundary |
| Derived recovery is mistaken for authoritative repair | WP-070 rewrites corruption, outbox declares ready with Delivering rows, projection repairs a published generation, or WP-185 collapses all findings into one readiness bit | WP-070/160/170/185/190 | Corruption is hidden, effects replay incorrectly, rebuild evidence is invalid, or a healthy authoritative database is taken down | WP-070 reports only bounded authoritative/derived findings; no authoritative online repair; WP-160 idempotent normalization; WP-170 new-generation rebuild; WP-185 separate health; crash matrix | Selected implementation, with evidence impact on POC recovery claims |
| Control-plane write bypass | Catalog/capability code mutates redb independently of coordinator with unclear sequence/audit | WP-050/070/110/120 | Violates sole-mutation boundary or creates untracked authority changes | Classify control-plane state; if authoritative, route it through the commit coordinator. Any exception requires a human amendment to the boundary; never allow direct API storage. | Core architecture boundary |
| Final composition drifts across component ports | WP-185 replaces a WP-130 provider/readiness gate, needs semantic changes, or constructs a second service/policy path | WP-130/140/160/170/180/185 | Individually correct components fail or bypass policy in the real process | Freeze registration/lifecycle ports upstream; keep WP-185 an extension of the tested P1 graph; return semantic fixes to owners; rerun the WP-130 restart suite in WP-185 acceptance | Selected work-package design |
| Comparison interface erases RiffDB semantics | Shared code begins exposing storage CRUD or only the guarantees PostgreSQL provides cheaply | WP-045/125/135/200 | Example becomes misleading and pressures core APIs toward a least-common denominator | Share workload/oracle and normalized observations only; keep backend adapters separate and forbid example dependencies from RiffDB crates | Evidence design risk; core concept if it shapes production APIs |
| Comparison benchmark is not equivalent | Backends use different durability, isolation, transports, guarantee profiles, warmup, pooling, or datasets | WP-045/125/135/150/200 | Misleading performance or complexity claims | Correctness preflight, explicit guarantee profiles, matched workload controls, separate component/process results, complete environment metadata | Evidence risk only |
| PostgreSQL comparison leaks into the critical path | A `riffdb-*` crate imports the driver/SQL or CI makes PostgreSQL required for core semantic tests | WP-045/125/135 and all production WPs | Violates architecture scope and couples RiffDB to an external database | Isolated private example package, dependency architecture check, separate pinned service-container CI tier | Selected implementation boundary |
| Observability leaks sensitive values | Raw keys/claims/free text appear in spans, metrics, MCP text, errors, or snapshots | WP-010/110/120/140/180 | Security failure and misleading model-facing content | Redact before subscriber, bounded labels, static descriptions, safe rendering, canary tests | Implementation/security |
| Recovery harness gives false confidence | Tests use recoverable errors/sleeps, omit indexes/outcomes/events/provenance/frontier, or inspect after repair | WP-070/100/160/170/190 | Torn states survive despite green tests | Named barriers, real process kill, full durable inspector, reference prefix comparison, repeated reopen | Evidence risk; concept if failures appear |
| Redb-specific bottleneck mistaken for concept failure | Single-writer/storage flush dominates but semantic model costs are not isolated | WP-070/100/200 | Wrong go/no-go decision | Component/process benchmarks, same semantic suite on approved Fjall adapter, attribute bottleneck | Selected implementation only |

## 11. Decisions Requiring Human Input

The consolidated 2026-07-13 approvals plus the accepted 2026-07-14
ADR-0004/ADR-0011 clarifications resolve the architecture decisions needed
to complete amended WP-010 and to start/complete WP-040, WP-060, WP-080,
WP-090, and their downstream audit/recovery/UUID consumers, including ADR-0021's
exact service-audit target boundary. The following concrete
artifact, compatibility, dependency, or later-boundary reviews cannot be
pre-approved without seeing their exact output:

1. **During the first WP-040 interface PR:** review the generated canonical
   `FORMAT.md` and `JSON_SCHEMA_FORMAT.md` registries before their compatibility
   fixtures merge. The generator may produce the review artifact, but only the
   maintainer can approve it as the concrete realization of ADR-0013.
2. **During WP-065 and WP-127 interface PRs:** review exact durable and public
   Protobuf messages, field numbers, descriptor/schema hashes, golden bytes, and
   compatibility classifications. WP-065 review includes the concrete EventHash
   framed goldens and proof that every real canonical envelope fits its declared
   conservative per-class upper bound. The accepted ADRs assign ownership and
   known semantic preimages/tags; they do not pre-approve every future field
   number or codec implementation.
3. **During the WP-070 interface/recovery PR:** review the exact redb table/key/
   metadata mapping within the frozen semantic prefixes and the named failpoint,
   process-crash, reopen, and integrity matrix, including actual-envelope-bound
   enforcement and event-hash recomputation, before claiming durable acceptance.
   This does not reopen ADR-0004 or the approved redb version.
4. **Before WP-180 or WP-140, whichever starts first:** accept ADR-0008's
   remaining resource URI, HTTP audience, stdio transport, cursor presentation,
   and bounded result/presentation fixtures. ADR-0020 command names and
   ADR-0007 invocation-time authorization are already frozen.
5. **At WP-075 dependency review:** approve the Fjall version and its license,
   unsafe/native, and reproducibility posture. WP-045's exact PostgreSQL crate,
   JSON fixture crate, and server-image baseline is already accepted; any change
   requires a new review. Neither dependency may enter the root workspace.
6. **At the owning package dependency review:** approve the exact Tokio, Tonic,
   rmcp, and any other not-yet-reviewed production dependency graph before it
   enters the root lockfile. The accepted redb and capability graphs do not imply
   approval of adjacent libraries.
7. **Before WP-130 conformance freezes:** review how the exact encoded
   `riffdb.v1.PublicError` is carried on non-OK gRPC responses and freeze matching
   client/server malformed, bounds, status-class, and compatibility tests.
8. **Post-POC only:** decide whether code generation becomes a `riffdb contract
   generate` subcommand and whether privileged replay/repair warrants a separate
   binary. WP-000 and the POC do not create `riffdb-codegen` or `riffdb-replay`.

The maintainer resolved the TicketDesk junction-read question on 2026-07-29 by
accepting ADR-0054 and directing WP-275. The approved extension is limited to an
earlier bounded `many` field consumed by `in` as one component of a later
`many` binding's complete same-partition primary key, with canonical order,
closed bounds, and an explicit missing-target outcome. General SQL, arbitrary
joins, non-key fan-out, collection-as-scalar behavior, and implicit follow-up
queries remain outside the critical path.

The maintainer accepted the safe-application product rule on 2026-07-29 through
ADR-0055. Stable application authority is exact to named compiled commands and
named deployed queries; ad-hoc RiffQL and raw kernel operations require
separate credentials. WP-280 through WP-300 own the exact capability tags,
private proof type, cost-vector/fuel encoding, relationship and unique grammar/
IR, compatibility fixtures, migration, and negative application corpus. Those
concrete interface artifacts still receive the human reviews required by their
own `human_review_triggers`; the accepted rule does not pre-approve arbitrary
wire tags, durable keys, or incompatible encodings.

The maintainer accepted ADR-0056 on 2026-07-29 by directing implementation of
the complete planned Agent Application Alpha phase. WP-305 still reviews exact
application-manifest and generated-signature fixtures; WP-315 reviews public
error tags; WP-320 reviews batch transport/resume/provenance encodings; WP-325
reviews any package/crate change; and WP-330 reviews the TypeScript dependency
graph. WP-335 additionally stops for a separate accepted ADR before any
evidence-derived RiffQL grammar, IR, plan, authorization, cursor, storage, or
result semantic is implemented.

The former SPEC Section 22.2 defaults and the accepted 2026-07-14 clarifications are
now resolved architecture decisions:

- Grammar-v1 read-only commands are unjournaled: no command-idempotency record,
  persisted command outcome, or application `CommitSequence`; required service
  audit remains a separate administration-stream record and does not journal the
  read outcome.
- A command may read multiple logical conflict domains only within one declared
  `PartitionKey`; all mutation keys/domains are declared up front and every
  influential cross-domain observation is tracked and revalidated.
- Write-influencing indexed range reads remain outside grammar v1 unless an
  accepted bounded IR declares exact up-front conflict keys and epoch semantics.
- Additive/documentation-compatible contract changes follow the accepted
  compatibility registry; removal of an entity, field, command, outcome, event,
  or projection is rejected rather than implemented destructively.
- A candidate starts with the count ceiling only; current validation, mutation-
  affected epoch reads, exact sequence-free planning, and semantic/conservative
  capacity reservation all precede sequence assignment. Actual canonical
  envelopes must fit their retained class bounds before staging.
- `EventHash` uses the exact ADR-0011 EventId/EventTypeId/u32-length-framed
  canonical `Value::Record` preimage under `riffdb.event/v1`.
- One full `StoredOutcomeV1` envelope is both terminal idempotency state and
  persisted outcome; terminal commit atomically deletes pending without a
  tombstone or second outcome record, and startup validates exact
  outcome/commit/provenance reciprocity without repair.
- Contract state-machine source syntax, AST/HIR/IR nodes, transition
  instructions, and runtime execution are outside POC grammar/IR v1. WP-080 is
  limited to expressions, predicates, invariants, postconditions, commit checks,
  and the existing bounded mutation/event instruction stream.
- WP-060 has one maintainer-approved allowed-path exception for
  `crates/riffdb-types/src/codec.rs`: a borrowed whole-record encoder must retain
  the exact WP-010 canonical bytes while preventing clone-before-bound behavior.
  It does not reopen canonical value or durable-format semantics.

Two decisions intentionally remain later-bound:

1. **Remote TLS provider, before remote MCP alpha:** defer this POC-external
   dependency/platform ADR until the alpha boundary.
2. **Redb versus Fjall for MVP, at POC exit:** keep redb unless the unchanged
   workload, conformance, recovery, and benchmark evidence justifies a change.

The initial budget create/seed decision is no longer open; accepted ADR-0015 owns
it. UUIDv7 generation/source/replay is also no longer open; accepted ADR-0018
owns it, including the reviewed direct `getrandom` owner set. No unresolved
architecture-direction choice is known before P0. P1 retains
the concrete WP-130 `PublicError` carriage review above; P2 still requires
the remaining ADR-0008 resource/transport/presentation decision and the other
artifact/dependency reviews recorded above.

## 12. First Execution Handoff

This handoff is archived: it was executed as WP-000 commit `4407d59` against the
`3235477` baseline. It remains below to preserve the original planning deliverable
and must not be rerun against the current implementation branch.

```text
You are implementing WP-000 only in /home/kevin/dev/riffdb.

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
