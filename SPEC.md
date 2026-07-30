# RiffDB
## Standalone Rust POC Technical Specification

### Native MCP interface and roadmap to MVP

**Tagline:** *Vibe fast. Commit safely.*  
**Category:** Contract-first operational database for agent-built applications  

**Version:** 0.45
**Status:** Application-platform implementation handoff
**Date:** 30 July 2026
**Audience:** Coding agents, database engineers, compiler engineers, security reviewers, and technical product leads  
**Working binaries:** `riffdbd`, `riffdb`, `riffdb-mcp`  
**Working URI scheme:** `riffdb://`  
**Source concept:** *Agentic OLTP Database — Concept Design* (pre-name concept document), Draft, July 2026

---

## Document control

| Field | Value |
|---|---|
| Technical owner | TBD |
| Product owner | TBD |
| Repository | `riffdb` |
| Primary language | Rust |
| POC deployment | Standalone, single-node server |
| Primary application API | RiffQL and generated named operations over gRPC |
| Native agent API | Model Context Protocol (MCP) |
| Storage baseline | `redb`, behind a narrow internal storage interface |
| Rust baseline | Rust 1.97.0 |
| MCP baseline | MCP specification 2025-11-25 |
| MCP Rust SDK baseline | `rmcp` 2.2.0 exactly |

### Revision history

| Version | Date | Summary |
|---|---|---|
| 0.1 | 2026-07-12 | Initial RiffDB implementation handoff specification for a standalone Rust POC and gated path to MVP. |
| 0.2 | 2026-07-12 | Reconciled the canonical contract grammar and gRPC surface; fixed commit, idempotency, control-plane, capability, MCP, gate, evidence, and work-package ownership decisions approved after the initial planning review. Associated ADRs remain Proposed until separately reviewed and accepted. |
| 0.3 | 2026-07-13 | Applied accepted ADR-0013 through ADR-0016: explicit binding-failure outcomes and command-only budget seeding, stable IR/hash/key boundaries, typed partition identity, and complete index-entry framing. |
| 0.4 | 2026-07-13 | Applied accepted ADR-0001, ADR-0004, ADR-0007, ADR-0009, ADR-0012, and ADR-0017: semantic storage and service boundaries, evaluated-command assembly, capability/bootstrap and audit semantics, deterministic execution-failure admission, projection identities/generations/frontiers, nonzero stable IDs, the exact WP-010 execution-failure error-schema carve-out, formal WP-065/WP-127 schema ownership, and the reconciled P1 dependency graph. |
| 0.5 | 2026-07-13 | Applied the accepted audit-clock/lifecycle, fail-closed recovery/readiness, crash-safe bootstrap-file, implicit-initial-outbox-state, initialization-mode, and ADR-0018 UUIDv7 generation/replay decisions before completing WP-010. |
| 0.6 | 2026-07-13 | Applied accepted ADR-0019 and ADR-0020: retained exactly the six authoritative POC metadata categories with unconditional startup integrity validation, and froze compiler-owned MCP command tool-name normalization, collision rejection, and catalog revalidation without accepting the remaining ADR-0008 URI or transport decisions. |
| 0.7 | 2026-07-13 | Applied accepted ADR-0021's exact lineage-scoped service-audit target registry and canonical ordering, and reconciled the canonical `LegalSpend`/`AllocateBudget` source identifiers with ADR-0020's no-word-splitting MCP name. |
| 0.8 | 2026-07-13 | Reconciled the accepted first-runnable P1 boundary: storage owns complete structural evidence and dormant type-state ports, catalog owns same-session IR-aware historical validation, WP-130 composes the minimal production `riffdbd` startup/lifecycle and restart proof, and WP-185 only extends that graph with P2 components. |
| 0.9 | 2026-07-13 | Clarified schema-complete startup evidence for every persisted entity, index-entry, and range-prefix key without a storage-to-IR dependency, and assigned ordered commit scans a dedicated 16 MiB internal encoded-content page ceiling while retaining the generic 4 MiB scan ceiling. |
| 0.10 | 2026-07-14 | Applied the accepted pre-sequence transaction-capacity clarification and exact `EventHash` preimage: mutation-affected epochs and conservative encoded capacity are resolved inside the write transaction before sequence assignment, while every final canonical `StoredEnvelope` is checked against its retained per-class bound before staging. |
| 0.11 | 2026-07-14 | Froze the accepted POC durable table keys and 26-record registry, including authoritative standalone event rows and lineage-scoped bundle keys; approved the pure-Rust backup checksum dependency and dependency-free storage benchmark harness; and clarified shared pure input-expression evaluation plus fail-closed pre-admission arithmetic. |
| 0.12 | 2026-07-14 | Clarified that one full `StoredOutcomeV1` envelope is both the committed terminal idempotency row and persisted outcome, with atomic pending deletion, no tombstone or second terminal envelope, and startup commit reciprocity; explicitly deferred contract state-machine source, IR, and execution from POC grammar/IR v1 and narrowed WP-080 to predicate, invariant, postcondition, and commit-check evaluation. |
| 0.13 | 2026-07-20 | Applied accepted ADR-0022's exact self-contained durable semantic Protobuf modules, 26-payload registry, field/tag/presence rules, canonical-wire validation, complete terminal outcome, and checked storage-codec boundary before WP-065 implementation. |
| 0.14 | 2026-07-20 | Applied accepted ADR-0023's exact WP-100 coordinator edge semantics: commit evaluator ownership, bounded full reevaluation, audit-input inversion, provenance attempt recovery, grammar-v1 empty index covered values, commit-check arithmetic classification, absent-target capability revocation, and no-transition control-plane audit classification. |
| 0.15 | 2026-07-20 | Clarified ADR-0023's audit-input inversion: the consumer view has no field or method for the new audit record's assigned administration sequence, while its checked control-plane result link may carry the sequence of an already-authoritative transition. |
| 0.16 | 2026-07-20 | Applied the accepted zero-mutation command clarification and direct gRPC public-error carriage: all influential dependencies remain revalidated, only exact nonzero mutation coverage invokes commit checks/index derivation, and WP-130 carries bounded `riffdb.v1.PublicError` bytes directly with closed status/message validation. |
| 0.17 | 2026-07-20 | Froze the accepted WP-100 command-executor result, error, completion-ownership, and cancellation boundary; and approved the narrow Tonic 0.14.6 dependency graph whose transport-only feature unification enables `base64` 0.22.1 `std` without changing auth's sole direct ownership or admitting TLS, compression, cryptography, or another base64 version. |
| 0.18 | 2026-07-20 | Applied the accepted checked-plan derived-index ceilings and index-epoch exhaustion behavior: conservative IR-v1 admission rejects plans whose successful mutation shape can exceed pre-sequence storage bounds, while runtime guards remain defense in depth and an exhausted epoch aborts before sequence assignment, stops the coordinator, and never enters uncertain-write fencing. |
| 0.19 | 2026-07-20 | Applied the accepted bounded active-lineage materialization clarification: catalog resolution proves exact parent/hash ancestry and optional-null field introductions under fixed count, canonical-byte, and process-local proof ceilings; commit normalizes both owned snapshots and transaction-current records before evaluation; runtime rejects every unproved omission; and activation/startup fail closed without changing durable, protocol, IR, or plan-hash formats. |
| 0.20 | 2026-07-20 | Clarified the accepted projection side of compatible event-payload evolution: catalog owns the opaque process-local event-materialization view used by projections, projection depends directly on catalog and consumes only that normalized view, and immutable event bytes/hashes plus the IR-blind storage boundary remain unchanged. |
| 0.21 | 2026-07-21 | Applied the accepted lineage-overflow retention/recheck and explicit coordinator-durability decisions: catalog owns opaque command-materialization readiness/resource evidence and the pure current-recheck API; over-budget returned evidence retains only the raw snapshot and exact resolved plan/proof and must reproduce the overflow after dependency and raw-observation equality; production coordinator construction explicitly receives `sync` or `group`, `memory` remains test-only, P1 `riffdbd` passes a code-level `sync`, and no constructor/backend default is inferred. |
| 0.22 | 2026-07-21 | Clarified the accepted three-attempt boundary: each attempt slot is consumed immediately before catalog snapshot materialization; a valid lineage-expansion `ResourceLimit` consumes that slot even though runtime is not entered, while a `Ready` result continues into exactly one runtime evaluation in the same slot. |
| 0.23 | 2026-07-21 | Applied the maintainer-approved pre-bootstrap health clarification: the unchanged API-neutral Health operation may use a private principal-less read-only context only during startup validation/bootstrap and returns only bounded lifecycle, liveness, and readiness; after the bootstrap marker it requires authenticated current-policy handling, while bootstrap remains the sole principal-less mutation and durable-audit exception. |
| 0.24 | 2026-07-21 | Applied accepted ADR-0031 and ADR-0032: transports preserve structurally checked pre-schema submitted command values for service-owned materialization under the exact selected plan, and the completed structural startup handoff carries the existing same-snapshot retained metadata needed for bootstrap lifecycle, active-pointer agreement, and allocator readiness. No public or durable format changed. |
| 0.25 | 2026-07-21 | Applied accepted ADR-0033: the coordinator publishes only first-durable application commit sequences through an injected least-authority sink into the server-owned bounded subscription hub; replay, read-only, failed, audit, initialization, and control-plane paths never publish. Sink defects preserve the known committed result but stop coordinator admission and process readiness. No public or durable format changed. |
| 0.26 | 2026-07-21 | Applied accepted ADR-0034 through ADR-0036: startup uses a Health-only initializing service plus move-only activation authority; authoritative index and commit scans return atomic frozen fences including an explicit before-first index position; and public query components remain structurally submitted until schema-directed service materialization. The not-yet-served public index-fence message is corrected and regenerated before WP-130. |
| 0.27 | 2026-07-21 | Applied accepted ADR-0037: the Rust SDK may depend directly, with default features disabled, on `riffdb-errors` and `riffdb-types` for the single checked public-error and foundational public-identifier/value owners. This grants no dependency on authority-bearing server crates and changes no public or durable format. |
| 0.28 | 2026-07-22 | Applied accepted ADR-0008, ADR-0039, ADR-0040, and ADR-0041: exact native-MCP transport, resource, schema, and bounded-session behavior; an isolated V2 index descriptor with a 27-readable/26-writable registry and restartable linear migration; the six-operation public gRPC parity bridge and WP-137 gate; and the public-only CLI, comparison-runner, credential, retry, configuration, and versioned JSONL boundaries, while reserving WP-155 for separately reviewed public backup/restore administration. |
| 0.29 | 2026-07-22 | Applied accepted ADR-0042: catalog now owns the sealed, backend-branded index-migration instruction and driver chain; storage API retains only semantic evidence and identity-only startup contracts; memory/redb use one narrow migration-only catalog edge with private backend transitions; and server only joins and drives matching outcomes. No durable or public protocol format changed. |
| 0.30 | 2026-07-23 | Applied accepted ADR-0043: auth owns one non-cloneable, nonserializable, redacted, zeroizing 43-byte retained opaque credential helper; Streamable HTTP may bound and copy bearer-stripped bytes only to borrow `OpaqueCredential` for the unchanged `CredentialAuthenticator`, while all authentication and authorization decisions remain on their existing shared paths. No dependency owner, wire format, durable format, or MCP registry changed. |
| 0.31 | 2026-07-23 | Applied accepted ADR-0044: the service owns a closed hosted-MCP request-context constructor with fixed MCP HTTP ingress and empty claims; the MCP adapter directly consumes only foundational types/errors plus narrow tracing; stdio uses a current-thread runtime and a non-reloadable rmcp telemetry filter. No policy edge, request-context claim carrier, wire field, durable field, or MCP registry changed. |
| 0.32 | 2026-07-23 | Applied accepted ADR-0045: added the isolated non-gating WP-139 budget-safety counterexample track, four exact PostgreSQL-negative-control/public-RiffDB contrasts, a bounded non-shipped evidence runner/report, and a WP-200 dependency while preserving the canonical PostgreSQL benchmark adapter, the frozen public runner, product binaries, production dependencies, contracts, and public/durable formats. |
| 0.33 | 2026-07-23 | Applied accepted ADR-0046: corrected the WP-137/WP-140 public presentation boundary with additive decimal precision and bounded contract-compatibility metadata, exact name-only enum submission, service-owned schema-bound outcome names, complete resource content and factual generated examples, plus hosted-only post-authentication MCP limiting. No durable, IR, bundle, plan-hash, URI, RPC, storage, or dependency boundary changed. |
| 0.34 | 2026-07-23 | Applied accepted ADR-0047 and ADR-0048: corrected thirteen pre-release MCP schemas to advertise their required object root without changing accepted instance shapes, and separated the existing 14-per-tick logical observer-operation bound from an enforced 46-per-tick/8,280-per-session physical service-call bound while removing the redundant stdio parity probe. No durable, IR, compiler, contract, public Protobuf, URI, or storage boundary changed. |
| 0.35 | 2026-07-24 | Applied accepted ADR-0049 and ADR-0050: completed bounded derived-worker recovery observations, catalog-owned event materialization and projection expression ownership, closed owner telemetry and hosted-MCP server composition; activated WP-155 with three public offline maintenance operations, a checksummed external receipt ledger, staged-backup authorization, and the explicit restore-rewind limitation. |
| 0.36 | 2026-07-28 | Applied accepted ADR-0051 through ADR-0053: established bounded symbolic RiffQL, exact-contract immutable query modules, an additive application API above the compatible kernel gRPC surface, one-snapshot composite query execution, rebuildable current catalog/capability views, and measured unary-gRPC-first application performance gates through WP-270. |
| 0.37 | 2026-07-29 | Applied accepted ADR-0054 and WP-275: added bounded collection-to-complete-key dependencies with explicit missing-target outcomes, closed dependent point batching inside one snapshot, and full one-request TicketDesk detail-page parity without general SQL joins. |
| 0.38 | 2026-07-29 | Applied accepted ADR-0055 and planned WP-280 through WP-300: separated stable named-application, scoped ad-hoc-agent, and kernel authority; required private derived query proofs, whole-request execution fuel, declared relationship and uniqueness integrity, safe generated defaults, and negative application canaries. |
| 0.39 | 2026-07-29 | Planned the proposed post-WP-300 Agent Application Alpha gate and WP-305 through WP-340: complete generated bindings, symbolic roles, structured public application errors, resumable command batches, canonical scaffolding and package boundaries, TypeScript runtime parity, evidence-driven RiffQL growth, and four sealed unfamiliar-domain agent evaluations before operational alpha or replication. ADR-0056 remains Proposed pending exact human acceptance. |
| 0.40 | 2026-07-29 | Applied accepted ADR-0056 after the maintainer directed implementation of the complete Agent Application Alpha phase. WP-305 through WP-340 may begin in dependency order; their exact public/durable/grammar fixtures retain the human review checkpoints named by the accepted record. |
| 0.41 | 2026-07-29 | Applied accepted ADR-0057 after four sealed Terra evaluations failed the application-authoring gate: authors own symbolic intent, the compiler owns an exact lock and every derived identity, diagnostics remain source-spanned and bounded, scaffolding supports an existing empty repository, TypeScript and builder MCP become complete public paths, public-only rehearsals and canaries precede campaign 02, and growing-database command degradation receives an independent durability-preserving gate. |
| 0.42 | 2026-07-29 | Applied accepted ADR-0058 and planned WP-364: redb retains `Immediate` two-phase durability behind one typed FIFO writer scheduler; production uses bounded group durability; command `Started` plus `Pending` admission and successful command graph plus terminal audit become two atomic transitions; every grouped command retains independent identity, outcome, provenance, audit, acknowledgement, and uncertainty recovery. |
| 0.43 | 2026-07-29 | Applied accepted ADR-0059 and planned WP-366: commands may carry compiler-proven same-partition observations across aggregates while every create/mutate binding remains in exactly one mutation aggregate; conflict keys derive only from that aggregate; external reads remain exact transaction-current dependencies; and the public TicketDesk seed plus unary mutation must demonstrate same-run PostgreSQL write parity within 2x. |
| 0.44 | 2026-07-30 | Applied the maintainer-approved ADR-0059 amendment: the internal FIFO writer may form compatible physical groups up to the existing 64-command transaction ceiling while the public transport batch remains capped at 16. Exact conflict/dependency checks, independent command semantics, two durable transitions, redb `Immediate` durability, and the 16 MiB transaction ceiling remain unchanged. |
| 0.45 | 2026-07-30 | Applied accepted ADR-0060 and planned WP-367 through WP-370: bounded oldest-item group collection, grouped historical idempotency selection, count-and-byte queue bounds, concurrent redb MVCC reads, exact transient outbox readiness, immutable cached command/query artifacts, bounded parallel deterministic preparation, and strict same-run application parity evidence while retaining one admission-ordered writer, fresh authorization, transaction-current validation, two Immediate durable transitions, and every fail-closed recovery guarantee. |
| 0.46 | 2026-07-30 | Applied accepted ADR-0061 and planned WP-371 through WP-375: application durability is a semantic acknowledgement and visibility guarantee; normal synchronous commands may transition atomically from vacant identity to terminal graph after bounded side-effect-free preparation; the standard application profile uses redb Immediate one-phase checksum commits while a hardened two-phase oracle remains; storage format V2 compacts per-row framing and references one authoritative event payload; conservative partition/index generations replace prefix fan-out; and visible non-durable chaining remains prohibited. |

### Normative language

The terms **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative. Requirement identifiers such as `TXN-004` are stable references for issues, pull requests, tests, and agent work packages.

### Scope labels

- **POC**: Required to demonstrate the core contract-first thesis on a durable single node.
- **AGENT-ALPHA**: Required to prove unfamiliar application authorship through
  public symbolic surfaces before operational or distributed alpha work.
- **MVP**: Required for an externally usable early product with replication, production authorization, operational tooling, and a stable compatibility story.
- **POST-MVP**: Deliberately deferred research or scale work.

---

# 1. Executive summary

RiffDB will begin as a standalone database server written in Rust. It will not layer transaction semantics on PostgreSQL. It will own the contract compiler, deterministic command runtime, logical conflict manager, commit protocol, durable state, idempotency records, typed outcomes, provenance, outbox, projection frontier, gRPC API, and native MCP API.

The POC will deliberately reuse mature Rust libraries for storage mechanics, networking, serialization, parsing infrastructure, observability, and testing. The product will own the semantics that are central to the thesis: business-state contracts, command-only mutation, conflict domains, invariant enforcement, retry behavior, effect restrictions, locality declarations, typed outcomes, agent capabilities, and transaction-to-projection consistency.

The POC is successful when it proves all of the following in one end-to-end adversarial demonstration:

1. A business invariant is declared in a contract and enforced under concurrency.
2. Two commands contending on the same logical business resource produce one successful typed outcome and one declared rejection, without an invalid committed state.
3. A command that commits but loses its response can be retried with the same idempotency key and returns the original persisted outcome without repeating the mutation or event.
4. Non-transactional effects cannot execute inside retryable command logic; only durable outbox intent may be emitted.
5. A commit sequence can be used to wait for a derived projection to include that commit.
6. The active contract dynamically becomes an MCP tool catalog with generated input and output schemas.
7. Every mutation can be traced to actor, agent session, request, contract version, plan hash, command, outcome, and affected records.
8. Restart and injected crash testing recover a state consistent with the durable commit log.

## 1.1 POC and MVP boundary

| Capability | POC | MVP |
|---|---|---|
| Deployment | Single durable node | Replicated cluster with tenant-local partition leaders |
| Writes | Contract commands only | Contract commands only; privileged audited repair path |
| Transaction class | Local transaction only | Local transaction only across physical partitions; no general distributed writes |
| Concurrency | Declared logical exclusive conflict keys plus version revalidation | Same semantics, replicated and partition-aware |
| Storage | `redb` baseline; benchmark against Fjall at POC exit | Selected engine with stable format, backup, restore, compaction, and upgrade policy |
| Contract language | Small typed DSL, no unbounded loops | Versioned language with migration and compatibility rules |
| API | Rust SDK, gRPC, CLI, native MCP | Stable Rust SDK, generated TypeScript/Python clients, gRPC, MCP, admin API |
| MCP | Stdio and loopback Streamable HTTP; dev capability tokens | Remote Streamable HTTP, OAuth 2.1, tenant and field policy, audit and approval flows |
| Projections | Filter, count, sum, grouped count/sum from events | Durable projection lifecycle, bounded joins, backfill, operational controls |
| Branching | Not required | Contract and representative-data branches; semantic replay and diff |
| Replication | None | OpenRaft-based replication behind an internal facade |
| SQL | None | Read-only SQL exploration is optional and not an MVP gate |
| Distributed transactions | None | None; explicitly post-MVP |

---

# 2. Goals, success criteria, and non-goals

## 2.1 Product goals

- Make business-state mutations expressible as typed commands rather than unrestricted row updates.
- Make conflict scope, retry semantics, side effects, and declared outcomes part of the database contract.
- Give the runtime enough information to enforce supported invariants atomically.
- Make agent access capability-scoped, attributable, and inspectable.
- Expose contracts as native MCP tools and context resources without a separate adapter product.
- Preserve a clean path from a single-node semantic kernel to replicated, partitioned execution.

## 2.2 POC success criteria

| ID | Criterion | Verification |
|---|---|---|
| POC-001 | The annual budget example compiles, deploys, and executes from the CLI, gRPC, Rust SDK, and MCP. | End-to-end test and recorded demo script. |
| POC-002 | With 100 units remaining, two concurrent 80-unit allocations cannot both commit. | Deterministic concurrency test and model comparison. |
| POC-003 | Every committed state satisfies all supported invariants. | Property tests over generated command histories. |
| POC-004 | A post-commit connection loss followed by retry returns the original outcome with `replayed=true`. | Failpoint integration test. |
| POC-005 | One logical command creates at most one mutation set and one durable event set for a given idempotency key. | Storage assertions after crash and retry. |
| POC-006 | The projection frontier is monotonic and `after_sequence=N` never returns a result missing commit N. | Projection property and integration tests. |
| POC-007 | Contract deployment changes the MCP command tool catalog and emits a list-changed notification when supported. | MCP integration test. |
| POC-008 | All mutation paths pass through shared authorization, command execution, and provenance code regardless of API. | Architecture test and code review gate. |
| POC-009 | The server recovers from every defined failpoint without torn committed state. | Process-kill recovery matrix. |
| POC-010 | The repository contains reproducible builds, CI, security checks, documented ADRs, and a one-command local demo. | Release checklist. |

## 2.3 Performance intent

POC performance work is for architectural feedback, not marketing claims. The POC MUST publish repeatable benchmark results for:

- Conflict-free command throughput and p50/p95/p99 latency.
- Hot-key contention with 1, 8, 32, and 128 concurrent callers.
- Idempotent replay latency.
- Commit-log scan and projection catch-up throughput.
- Restart recovery time at several database sizes.
- Synchronous durability and group-commit durability modes.

The POC has no absolute TPS release gate. A measured bottleneck that originates in the semantic design is a go/no-go input; a bottleneck confined to a replaceable embedded engine is not by itself a concept failure.

## 2.4 Non-goals for the POC

- General SQL parsing, planning, or unrestricted SQL writes.
- Distributed consensus, replication, failover, or multi-region behavior.
- General distributed transactions.
- Online repartitioning.
- Arbitrary host-language callbacks in transaction bodies.
- External network access inside command execution.
- Arbitrary joins, windows, distinct counts, or approximate analytics.
- Automatic inference of business invariants from application code.
- A production OAuth authorization server.
- Git-like merging of divergent transactional histories.
- A competitive general-purpose B-tree, LSM tree, page cache, TLS stack, or RPC framework.

---

# 3. Fixed design decisions

These decisions are binding for the POC unless changed through an ADR reviewed by a human maintainer.

| Decision | Requirement | Rationale |
|---|---|---|
| Standalone database | `SYS-001` The POC MUST run as its own durable server process. | Proves the proposed application/database contract without inheriting another database's transaction surface. |
| Rust first-party code | `SYS-002` All first-party production code MUST be Rust. | Keeps compiler, runtime, storage coordination, and agent interface in one type and tooling ecosystem. |
| Safe Rust default | `SYS-003` Workspace crates MUST use `#![forbid(unsafe_code)]` unless an ADR grants a narrow exception. | Reduces the correctness surface in a database prototype. |
| Command-only writes | `SYS-004` Application roles MUST NOT have a generic insert, update, or delete operation. | Prevents bypass of invariants, state machines, idempotency, outbox, and provenance. |
| Custom typed DSL | `CMP-001` Contracts MUST compile from a small declarative language into a versioned executable IR. | Enables precise dependency, outcome, schema, and MCP generation. |
| Conflict-key locking | `TXN-001` POC mutation authority MUST be enforced with exclusive logical conflict keys acquired before command evaluation. | Gives borrow-like semantics a precise runtime meaning while keeping implementation understandable. |
| Commit revalidation | `TXN-002` All read versions and commit-time predicates MUST be revalidated in the durable write transaction. | Prevents dependency escape when commands read outside the primary conflict record. |
| Single commit sequence | `LOG-001` Every committed declared command outcome MUST receive a monotonically increasing nonzero `CommitSequence`, including persisted no-op business outcomes. ADR-0012 `ExecutionFailed` is a terminal idempotency-admission resolution, not a command outcome or application commit, and receives no sequence. | Unifies committed outcomes, provenance, change consumption, and projection frontiers without misclassifying a proven pre-commit execution failure. |
| Redb baseline | `STO-001` The first storage implementation MUST use `redb` behind a narrow semantic storage trait. | Provides pure-Rust ACID storage with simple single-writer commit behavior. |
| Versioned Protobuf records | `STO-002` Durable records and public gRPC messages MUST use explicit versioned Protobuf schemas. | Supports compatibility, generated types, and future replicated-log entries. |
| Shared service core | `API-001` gRPC, CLI, Rust SDK, and MCP MUST invoke the same application service and authorization layer. | Avoids semantic drift and privileged side paths. |
| Closed application authority | `SAFE-001` through `SAFE-003` separate exact named stable-application operations, explicitly granted ad-hoc agent queries, and raw kernel administration. | Makes reviewed application behavior enforceable instead of relying on SDK convention or table-shaped permissions. |
| Native MCP | `MCP-001` MCP MUST be an in-repository first-class interface, not a separate integration maintained against internal APIs. | The product is designed for autonomous agents and should expose its semantics directly. |
| No replication in POC | `REP-001` The POC MUST preserve replicated-state-machine-shaped boundaries but MUST NOT implement Raft. | Prevents consensus engineering from obscuring the semantic proof. |

---

# 4. Logical architecture

[Graphviz source — POC logical architecture](diagrams/poc_architecture.dot)

## 4.1 Components

| Component | Responsibility | Must not own |
|---|---|---|
| Contract syntax | Lexing, parsing, source spans, syntax AST | Storage or runtime behavior |
| Contract compiler | Name resolution, type checking, invariant classification, dependency analysis, JSON Schema generation, plan generation | Network transport or data mutation |
| Contract catalog | Bundle lookup, compatibility checks, and validated typed deployment operations; the coordinator persists bundles and active-version changes | Parsing source text during command execution or direct authoritative writes |
| Authorization and policy | Principal resolution, capability checks, tenant/field constraints, approval requirements | Command semantics |
| Command application service | API-neutral semantic validation, exact plan/input preparation, current authorization, security-audit orchestration, invocation of typed command/control-plane executors and bounded read ports, obligation application, and safe result release | Transport formatting, conflict acquisition, runtime evaluation, storage transactions, sequence assignment, or authoritative writes |
| Deterministic runtime | Evaluate a compiled command plan against an owned snapshot and produce an `EvaluatedCommand` or closed execution fault | I/O, clocks, random OS state, provenance claims, admission persistence, network calls, or `CommitIntent` assembly |
| Conflict manager | Canonical conflict-key acquisition, wait queues, cancellation, fairness, hot-key metrics | Durable state |
| Commit coordinator | After acceptance of an authorized typed preparation, create or resolve admission, acquire logical capabilities, materialize bounded snapshots, invoke deterministic runtime, revalidate, invoke the narrow policy-owned transaction-current capability verifier where required, assign application or administration sequences, atomically apply authoritative records, and notify bounded consumers | Contract parsing, general service/transport policy decisions, duplicated capability-policy predicates, obligations/result redaction, or external effects |
| Storage engine | Atomic key/value transactions and ordered scans required by the semantic layer | Business rules |
| Commit log and provenance | Durable ordered record of committed declared outcomes and mutations | Projection-specific state or ADR-0012 terminal non-commit admission failures |
| Outbox worker | Deliver durable events with explicit delivery policy | Command transaction execution |
| Projection worker | Consume contiguous commits and advance a durable frontier | Direct mutation of source entities |
| gRPC API | Programmatic application and administration protocol | Alternative semantics |
| MCP API | Dynamic tools, resources, prompts, progress, cancellation, and agent-safe result shaping | Direct storage access |
| CLI | Local operator and demo workflows over public APIs | Hidden privileged mutation path |

## 4.2 Trust boundaries

1. Network input is untrusted until transport decoding, size checks, authentication, and the shared service's schema-directed validation complete. A transport's structural value validation does not claim compiled-schema materialization.
2. Contract source is untrusted until compilation succeeds and deployment policy approves the resulting plan.
3. MCP client and model behavior is untrusted; every tool call is authorization-checked and input-validated.
4. The deterministic runtime is trusted to evaluate only compiler-produced IR.
5. The commit coordinator is the only component allowed to assign commit sequences or atomically mutate durable application tables.
6. Outbox connectors are outside the transaction boundary and MUST be treated as unreliable, duplicate-prone external systems.
7. Projection state is derived and rebuildable; source state and the commit log are authoritative.

---

# 5. Rust workspace and dependency boundaries

## 5.1 Workspace layout

```text
riffdb/
  README.md
  Cargo.toml
  Cargo.lock
  rust-toolchain.toml
  deny.toml
  AGENTS.md
  SPEC.md
  work_packages.yaml
  diagrams/
  adr/
  contracts/
    examples/
    parser-fixtures/
  proto/
  fixtures/
  crates/
    riffdb-types/
    riffdb-errors/
    riffdb-proto/
    riffdb-contract-syntax/
    riffdb-contract-ir/
    riffdb-contract-compiler/
    riffdb-catalog/
    riffdb-storage-api/
    riffdb-storage-memory/
    riffdb-storage-redb/
    riffdb-invariant/
    riffdb-runtime/
    riffdb-conflict/
    riffdb-idempotency/
    riffdb-commit/
    riffdb-auth/
    riffdb-policy/
    riffdb-service/
    riffdb-api-grpc/
    riffdb-client-rust/
    riffdb-server/
    riffdb-api-mcp/
    riffdb-mcp-stdio/
    riffdb-cli/
    riffdb-outbox/
    riffdb-projection/
    riffdb-observability/
    riffdb-diagnostics/
    riffdb-testkit/
  tests/
    e2e/
    crash/
    conformance/
  benchmarks/
    storage-fjall/           # nested non-production Fjall comparison workspace
  examples/
    budget-comparison/       # isolated long-lived comparison workspace
      core/                  # backend-neutral workload and observation oracle
      fixtures/              # golden workload observations
      postgres/              # PostgreSQL comparison implementation
      riffdb-service/        # in-process service adapter added after WP-120
      riffdb-grpc/           # WP-135 package riffdb-budget-comparison-riffdb-grpc and riffdb-budget-public runner
      safety-evidence/       # WP-139 non-shipped counterexample report and runner
  scripts/
  docs/
  release/
    systemd/
```

The binary targets are `riffdbd` from `riffdb-server`, `riffdb` from `riffdb-cli`, and `riffdb-mcp` from `riffdb-mcp-stdio`.

## 5.2 Crate responsibilities

| Crate | Public responsibility | Allowed dependency direction |
|---|---|---|
| `riffdb-types` | Stable identifiers, pure checked UUIDv7 assembly, canonical value types, versions, timestamps, decimals, and common value semantics | Foundation crate; no clock, entropy, storage, or transport dependencies |
| `riffdb-errors` | Public-safe errors, internal error layering, redaction boundaries, incident identifiers, and the consumer-owned `IncidentIdSource` port | Types only; no transport-specific errors, clock, entropy, or concrete incident provider in core layers |
| `riffdb-proto` | Generated public/durable Protobuf types, envelopes, readable/writable durable registries, descriptors, wire validation, foundational value/error conversion helpers, and structural canonical outcome-locator validation | Types, errors, Prost, and the presentation-only exact `base64` edge; no storage API, runtime, service, or transport dependency |
| `riffdb-contract-syntax` | Lexer, parser, source spans, syntax AST, and parser diagnostics | Types and parser tooling only |
| `riffdb-contract-ir` | Typed HIR, executable IR, plans, immutable projection group schemas, and contract bundle structs | Types; no parser implementation, storage, compiler implementation, or runtime dependency |
| `riffdb-contract-compiler` | Name resolution, type checking, invariant classification, locality analysis, plan generation, and JSON Schema output | Syntax and IR; no storage, service, or transport dependency |
| `riffdb-catalog` | Immutable contract bundle persistence, compatibility checks, active-version changes, catalog notifications, bounded active-lineage materialization proofs, opaque command `Ready`/resource evidence and pure current-recheck operations, exact projection-plan resolution and opaque projection event-materialization views, IR-aware validation of bounded same-session historical startup evidence, historical V1/V2 index-partition derivation, and the fields-private backend-branded consuming index-migration driver/instruction/completion chain | IR, storage API, and the sole narrow pure `riffdb-invariant` uses accepted by ADR-0039 and ADR-0049; its opaque process-local proofs, evidence, migration context, instructions, completion, and materialization views never cross an operational storage trait or become constructible from public parts |
| `riffdb-storage-api` | Semantic snapshots, database initialization, structural startup sessions/evidence/type-state ports including the same-snapshot retained-metadata handoff, codec-owned migration semantic row/evidence, checked migration bounds/cursor, identity-only startup migration port contract and closed outcome, evaluated commands, commit intents, durable DTOs, typed transitions/readers, bounded exact-end derived-recovery observations and atomic projection query fences, the narrow durable `proto_codec` mapping bridge, and ADR-0017 projection-schema consumers | Types/errors; proto only through `proto_codec`; contract IR only for immutable `ProjectionGroupSchema`/`BoundProjectionGroupSchema` values in the projection-schema module; no catalog authority/tracker/instruction/completion, public migration read/apply/finish operation, clock, entropy, compiler, command-plan interpretation, historical-plan validation, runtime, commit, service, transport, or concrete-engine dependency |
| `riffdb-storage-memory` | Deterministic reference storage implementation used by model and semantic tests, including the named concrete startup-migration port used by catalog-driver conformance | Storage API, plus the sole ADR-0042 migration-only catalog driver edge; default-feature-disabled contract compiler only as a dev-dependency for canonical conformance fixtures; no production contract IR/compiler/invariant, historical partition-expression interpretation, `ValidatedCatalogHistory`, readiness composition, or API transport dependency |
| `riffdb-storage-redb` | Durable POC implementation, table layout, complete structural integrity/evidence scan, bounded derived scans/query fences, session-bound V1/V2 index evidence and exclusive compare-and-rewrite migration port, dormant opened ports, backup, restore, the external maintenance receipt adapter, and engine benchmarks | Storage API, `redb`, and the sole ADR-0042 migration-only catalog driver edge; default-feature-disabled contract compiler only as a dev-dependency for canonical recovery fixtures; no production contract IR/compiler/invariant, historical partition-expression interpretation, `ValidatedCatalogHistory`, readiness composition, or API transports |
| `riffdb-invariant` | Shared pure evaluation of checked input-computable expressions plus supported predicates, invariants, postconditions, and commit-time checks | Types and IR; no snapshot, admission, storage, service, clock, entropy, or transport dependency; no POC state-machine source, IR, or execution surface |
| `riffdb-runtime` | Deterministic command-plan interpreter that consumes lineage-normalized owned snapshots, rejects unproved missing fields, and produces `EvaluatedCommand` without external I/O | IR, invariant engine, and storage semantic value/snapshot types; no catalog lineage construction, provenance claims, admission persistence, storage engine, service, or transport dependency |
| `riffdb-conflict` | Canonical conflict keys, exclusive logical capabilities, wait queues, cancellation, and hot-key diagnostics | Types and synchronization primitives; no storage engine |
| `riffdb-idempotency` | Canonical command identity, input hashing, persisted outcome lookup, duplicate detection, and uncertain-result recovery | Types and storage API |
| `riffdb-commit` | Database-initialization executor, admission, capability acquisition, orchestration of catalog-owned snapshot readiness/resource evidence and current rechecks, deterministic evaluation orchestration, exact historical-plan matching, final `CommitIntent` assembly, transaction-current commit-check orchestration, revalidation, sequencing, authoritative commit, typed control-plane operations, ordered audit execution, and consumer-owned `AdmissionClock`, `AdministrationClock`, `ProvenanceIdSource`, and `AdministrationAuditInputView` ports | Runtime, conflict, idempotency, catalog, `riffdb-contract-ir`, `riffdb-invariant`, storage API, and policy-owned authorized-preparation/provenance/facts values plus only `TransactionCurrentCapabilityVerifier` and `AuthorizationClock`; direct IR/invariant use is limited to exact plan matching, grammar-v1 index derivation, and pure transaction-current commit-check evaluation; no syntax/compiler, service, transport, protocol, general policy authorizer, obligations/redaction engine, policy-owned storage reader, or concrete clock/entropy implementation |
| `riffdb-auth` | Principal authentication, local development capability tokens, expiry, credential resolution, the non-cloneable/nonserializable redacted hosted-MCP and offline-maintenance bounded zeroizing credential handoffs, narrow synchronous authentication clock, and `CredentialAuthenticator` entry point | Types, errors, existing auth-owned `zeroize`, and storage-owned capability readers; no policy, command execution, service, or transport dependency |
| `riffdb-policy` | Deny-by-default authorization, capability scopes, obligations, approvals, provenance validation, closed offline-maintenance authorization, value-only authorized capability-mutation preparation and transaction-current facts, synchronous authorization clock, and pure transaction-current capability verification | Auth and types/errors only; no storage API, commit, service, transport, protocol, authoritative write handle, or concrete storage dependency |
| `riffdb-service` | API-neutral command, contract, entity, commit, provenance, projection, conditional discovery, administration, health, and offline-maintenance services, including capability-administration request/result semantics, checked pre-schema submitted command values, schema-directed canonical command-input preparation, the immutable operation-schema catalog, authoritative outcome-locator tuple encoding/decoding, and closed gRPC/hosted-MCP request-context constructors | Foundational types/errors, contract/compiler/catalog semantics, the pure `riffdb-invariant` expression evaluator, auth and policy entry points, typed commit and maintenance executors, consumer-owned bounded read ports, and exact presentation-only `base64`; no runtime execution API, transport, general storage engine, or concrete storage implementation |
| `riffdb-api-grpc` | Tonic services, authentication interceptors, bounds checks, presentation-fence process-generation joining, and sole total service-to-wire conversion for all 25 public RPCs | Service, the auth-owned `CredentialAuthenticator` and maintenance credential-handoff interfaces, proto, and Tonic; no policy, catalog, runtime, commit, storage API, or storage implementation |
| `riffdb-client-rust` | Generic and generated Rust client APIs, the approved system UUIDv7 request/capability/agent-session/maintenance-operation convenience source, six parity-bridge methods, maintenance methods, operation-specific retry helpers, public-error re-exports, and one protected presentation-only bearer-file loader/comparison | Proto and Tonic client, default-feature-disabled `riffdb-errors` and `riffdb-types`, exact ADR-0018 entropy, and exact `zeroize`; no `base64`, `riffdb-auth`, authority-bearing, or semantic database implementation dependency |
| `riffdb-server` | `riffdbd` process composition, configuration, lifecycle, hosted gRPC/HTTP endpoints, startup and maintenance proof composition, and concrete OS clock/UUIDv7/cursor/digest/server-generation providers implementing the separate consumer ports | Service/API crates and concrete auth, clock/identifier, executor, storage, maintenance, outbox, projection, and observability implementations solely for composition; no new policy, command, query, redaction, cursor, audit, or identifier semantics |
| `riffdb-api-mcp` | The sole MCP SDK boundary: common bounded presentation DTOs/rendering, exact tool/resource/schema registries, locators, cursors, observers, structural base64 presentation, one checked hosted `RequestIdSource`, one common public-safe error renderer, and the hard rmcp tracing-target predicate; plus feature-gated Streamable HTTP authentication/session handling and stdio transport support | Ungated common code may consume only foundational `riffdb-types`, public-safe `riffdb-errors`, and tracing in addition to its presentation dependencies; it has no service/auth/server/policy/runtime/commit/catalog/storage edge; `streamable-http` alone may add optional service and auth-owned `RetainedOpaqueCredential`/`CredentialAuthenticator` edges; the retained helper grants only bounded copy and an opaque borrow, never token parsing or an auth decision; no direct policy, catalog, runtime, commit, storage API, or storage implementation |
| `riffdb-mcp-stdio` | `riffdb-mcp` local stdio bridge that uses the public Rust client and public gRPC only | Client, MCP stdio transport glue, and the exact non-reloadable tracing-subscriber install; no auth, service, server, policy, runtime, commit, catalog, or storage access and no gRPC server feature |
| `riffdb-cli` | `riffdb` operator and developer CLI using only public Rust-client operations, bounded local configuration/credential retention/rendering, the three offline-maintenance commands, and the checked budget-comparison runner | Exact ADR-0041 direct dependency allowlist; the sole auth-crate symbol boundary is isolated `riffdb_auth::bootstrap_secret`, never the root normal-token loader, authenticator, policy, storage, service, server, proto, or semantic internals |
| `riffdb-outbox` | Durable event dispatch state machine, leases, retries, deduplication metadata, and connectors | Storage API and Tokio; outside command execution |
| `riffdb-projection` | POC event-derived filters, counts, sums, durable frontiers, rebuilds, and read-after-sequence outcomes over catalog-normalized event views | Catalog, storage API, and ADR-0049's narrow default-feature-disabled IR/pure-invariant evaluator edges; no direct durable-event schema interpretation, ancestry/null-fill authority, command/commit-check execution, or storage implementation dependency |
| `riffdb-observability` | Structured tracing, metrics, health signals, and safe telemetry helpers | Cross-cutting interfaces without business semantics |
| `riffdb-diagnostics` | Bounded explain output, compiler/runtime diagnostic rendering, and safe operator-facing reports | Types, compiler plans, and observability helpers |
| `riffdb-testkit` | Reference model, generated histories, failpoints, temporary servers, recovery harnesses, and shared fixtures | May depend on implementation crates only in test contexts |

## 5.3 Dependency direction rules

- Compiler crates MUST NOT depend on runtime, storage, gRPC, or MCP crates.
- Runtime MUST NOT depend on a concrete storage engine.
- Storage implementations MUST NOT depend on gRPC or MCP.
- API crates MUST NOT access storage implementations directly.
- MCP and gRPC MUST share the `riffdb-service` authorization and execution entry points.
- Hosted MCP MUST construct ordinary service context only through
  `RequestContext::from_authenticated_mcp_http`. That constructor fixes
  `ServiceIngressKindV1::McpHttp`, constructs exactly five absent
  `UntrustedInvocationClaims`, and accepts only a checked `RequestId`, privately
  constructed `AuthenticatedPrincipal`, process-local `RequestControl`, and
  optional already-trusted `TraceContext`. MCP MUST NOT import policy to call
  the general constructor, label hosted traffic as gRPC, or derive a request
  ID, agent session, claim, or trace context from MCP protocol/session/progress
  identifiers, a credential, tool input, or an unreviewed header.
- MCP Streamable HTTP MAY construct the auth-owned
  `RetainedOpaqueCredential` only from an already bearer-stripped byte slice of
  at most 43 bytes and MAY use only its `borrow()` result as the
  `OpaqueCredential` passed to `CredentialAuthenticator`. Construction only
  bounds and copies; it performs no syntax check, decode, authentication, or
  authorization. The helper is non-`Clone`, nonserializable, redacted, backed
  by `Zeroizing<[u8; 43]>`, and exposes no raw data accessor. MCP session state
  MUST NOT retain it or its borrowed view. A live SSE response handler MAY own
  exactly one retained value for the handler lifetime and MAY borrow it only to
  perform the required fresh `CredentialAuthenticator` call before each
  emitted notification; the handler MUST release it on close and MUST NOT cache
  a principal, policy decision, obligation, or authorization result.
- `riffdb-proto` MUST NOT depend on `riffdb-storage-api`; only the narrowly scoped storage-owned `proto_codec` bridge may map semantic durable DTOs to generated messages.
- `riffdb-storage-api` MAY consume only the two immutable checked ADR-0017 projection schema values from `riffdb-contract-ir`; it MUST NOT consume `CommandPlan` or compiler/runtime services, and `riffdb-contract-ir` MUST NOT depend on storage.
- `riffdb-catalog` MAY consume `riffdb-invariant` only to evaluate one selected
  immutable historical aggregate `partition_expression` over already decoded
  canonical root-key values while validating or migrating a codec-checked index
  row. That call is pure, bounded, synchronous, storage-free, callback-free,
  and grants no command-execution, commit-check, or mutation authority. No
  reverse invariant-to-catalog or storage-to-IR dependency is permitted.
- `riffdb-storage-memory` and `riffdb-storage-redb` MAY consume
  `riffdb-catalog` only through ADR-0042's catalog-owned startup index-migration
  driver and fields-private move-only request/proof values branded by the exact
  concrete backend type. Every cross-crate scan, one-bundle read, batch apply,
  and finish call consumes one such branded value, and its backend response
  factory consumes that exact request. The server never receives these values.
  This edge grants no direct IR/invariant import, catalog persistence access,
  historical validation, readiness composition, or operational storage path.
  Both concrete backends MAY additionally use the default-feature-disabled
  contract compiler only as a dev-dependency to build real canonical indexed
  bundles for conformance/recovery fixtures; it creates no production compiler
  or IR edge.
- `riffdb-projection` MUST resolve projection semantics and materialize scanned
  event payloads through `riffdb-catalog`; it MUST NOT interpret a raw durable
  event against contract IR or create a second ancestry/null-fill path.
  ADR-0049 permits projection's direct default-feature-disabled
  `riffdb-contract-ir` and `riffdb-invariant` edges only to evaluate the
  already checked filter/group/measure expressions over catalog's opaque
  normalized view. Catalog remains the sole normalizer, invariant remains the
  sole pure expression evaluator, and storage remains IR-blind.
- Offline maintenance uses the API-neutral service and policy-owned
  authorization proof from ADR-0050. gRPC, the Rust client, and CLI receive no
  concrete backup/restore handle or path; MCP has no maintenance operation.
- The external `.maintenance` receipt subtree is owned only by the concrete
  offline maintenance adapter. It is neither a redb table nor a seventh
  database metadata category and never crosses an ordinary operational storage
  trait.
- gRPC and MCP HTTP MAY call only the auth-owned `CredentialAuthenticator` before constructing service request context; production MCP stdio, CLI, and SDK use public gRPC.
- Generated code MUST be checked in only when generation is deterministic and CI verifies it is current.

## 5.4 Baseline libraries

Versions are the verified July 2026 starting point, not a promise to track every release. The lockfile is authoritative.

| Area | Baseline | Use |
|---|---|---|
| Toolchain | Rust 1.97.0 | Workspace toolchain and CI baseline |
| Async runtime | Tokio 1.52.x | Networking, service tasks, channels, timeouts |
| gRPC | Tonic 0.14.6 with crate-specific default-disabled features fixed by ADR-0009 | Public RPC server and client without TLS or compression in the POC |
| Hosted HTTP | Axum 0.8.9, default features disabled, `http1` and `tokio` only | WP-185 loopback hosting of WP-140's accepted MCP Tower service |
| Protobuf | Prost 0.14.x | Wire and durable record generation |
| Pure-Rust proto compiler | Protox 0.9.x | Build without requiring an external `protoc` executable |
| Embedded storage | redb 4.1.0, default features disabled, no optional features | POC durable state and atomic commits; direct only in `riffdb-storage-redb` |
| Backup checksum | sha2 0.11.0, default features disabled | SHA-256 backup manifests; direct only in `riffdb-storage-redb` and never command semantics |
| Storage comparison | Fjall 3.1.x | POC-exit benchmark and possible MVP engine |
| MCP SDK | rmcp exactly 2.2.0, default features disabled | Solely through `riffdb-api-mcp`: unconditional `server`, `stdio` adds only `transport-io`, and `streamable-http` adds only `transport-streamable-http-server` plus optional service/auth edges |
| OS entropy | getrandom 0.3.4, default features disabled, no optional features | Capability/bootstrap identifiers and tokens in `riffdb-auth`, request/capability/agent-session convenience IDs in `riffdb-client-rust`, and injected database/provenance/request/incident/cursor IDs plus the independent 16-byte process generation in `riffdb-server`; never command runtime |
| Base64 presentation | base64 exactly 0.22.1, default features disabled, `alloc` enabled at every direct owner; accepted dependency unification may enable `std` transitively | Direct owners are exactly `riffdb-auth`, `riffdb-proto`, `riffdb-service`, `riffdb-api-mcp`, and `riffdb-cli`; only auth decodes credentials, while the other four own only their reviewed structural/presentation uses |
| Secret cleanup | zeroize exactly 1.8.1, default features disabled, `alloc` only | Direct owners are exactly `riffdb-auth`, `riffdb-client-rust`, and `riffdb-cli` for bounded credential buffers; no claim about environment, transport-generated, kernel, or allocator copies |
| Lexer | Logos 0.16.x | Contract tokenization |
| Parser | LALRPOP 0.23.x | Contract grammar |
| Diagnostics | Miette | Compiler errors with source spans and stable diagnostic codes |
| Tracing | tracing 0.1.x | Structured spans and events |
| Property testing | Proptest 1.x | Generated contracts, values, and histories |
| Exhaustive concurrency | Loom 0.7.x | Small lock-manager and notification components |
| Randomized concurrency | Shuttle 0.9.x | Larger command and task schedules |
| Benchmarks | Dependency-free repeated-run harnesses | Stable local component and scenario benchmarks without adding a POC Criterion dependency |
| Future replication | OpenRaft 0.9.x behind a facade | MVP replicated state machine; not linked into POC server |

`riffdb-api-mcp` has empty default features. Its `rmcp` manifest row is exactly
`rmcp = { version = "=2.2.0", default-features = false, features = ["server"] }`.
The crate feature `stdio` adds only `rmcp/transport-io`; `streamable-http` adds
only the optional `riffdb-auth` and `riffdb-service` edges plus
`rmcp/transport-streamable-http-server`. No rmcp client, sampling, roots,
elicitation, task, OAuth, TLS, key-value, or unrelated transport feature is
enabled. Acceptance of ADR-0008 covers only its recorded 2026-07-22 lock,
build-script, native, license, advisory, cryptographic, and unsafe inventory.
The real WP-140 lock and feature graph MUST receive a fresh human-visible review
and stop on any difference; in particular, the accepted scratch graph selected
`serde_json` 1.0.151 while ADR-0041 pins 1.0.150 for the CLI.
ADR-0043 adds no dependency owner: `RetainedOpaqueCredential` uses
`riffdb-auth`'s existing exact `zeroize` edge as
`Zeroizing<[u8; 43]>`. The direct `zeroize` owner set remains exactly
`riffdb-auth`, `riffdb-client-rust`, and `riffdb-cli`.

ADR-0044 adds these exact unconditional `riffdb-api-mcp` rows:
default-feature-disabled path dependencies on `riffdb-types` and
`riffdb-errors`, plus
`tracing = { version = "=0.1.44", default-features = false, features = ["std"] }`.
It adds
`tracing-subscriber = { version = "=0.3.23", default-features = false, features = ["fmt"] }`
to `riffdb-mcp-stdio`, whose direct Tokio features are exactly `macros` and
`rt`. No tracing-subscriber default, ANSI, JSON, env-filter, tracing-log, or
reloadable hard-filter owner is admitted. `riffdb-api-mcp` retains Tokio
`time` only behind the injected adapter-local scheduler; no semantic clock,
logical time, identifier, or command-runtime input is obtained from it.

ADR-0049 moves the already locked exact Axum 0.8.9 row into
`riffdb-server` production dependencies with only `http1` and `tokio`.
`riffdb-server` may add only Tokio `net` and `signal` to its existing direct
process features for the hosted listener and SIGINT/SIGTERM handling. This
admits no TLS, HTTP/2, proxy trust, remote bind, or network metrics exporter.

For WP-150, `riffdb-cli` has empty default features and exactly these direct
production dependencies: `base64 = 0.22.1` (`alloc`), `clap = 4.6.3`
(`derive`, `std`, `help`, `usage`, `error-context`), default-feature-disabled
path dependencies on `riffdb-auth` and `riffdb-client-rust`, `serde = 1.0.229`
(`derive`, `std`), `serde_json = 1.0.150` (`std`), `tokio = 1.52.0`
(`macros`, `rt-multi-thread`), `toml = 1.1.3` (`parse`, `serde`, `std`), and
`zeroize = 1.8.1` (`alloc`). All exact third-party rows disable default
features. The CLI has no direct Tonic or foundational/semantic crate edge.
Every first-party crate continues to forbid unsafe code; a differing resolved
CLI graph requires the same renewed dependency review before merge.

---
# 6. Core domain model and identifiers

## 6.1 Stable identifiers

The semantic model MUST use newtypes rather than raw strings or integers at component boundaries.

```rust
pub struct ContractVersion(NonZeroU64);
pub struct PlanHash(pub [u8; 32]);
pub struct ContractBundleHash(pub [u8; 32]);
pub struct ProjectionPlanHash(pub [u8; 32]);
pub struct ContractPlanRootHash(pub [u8; 32]);
pub struct CommitSequence(NonZeroU64);
pub struct AdministrationSequence(NonZeroU64);
pub struct EntityVersion(NonZeroU64);
pub struct IndexEpoch(NonZeroU64);
pub struct RequestId([u8; 16]);
pub struct ActorId(pub String);
pub struct AgentSessionId([u8; 16]);
pub struct IncidentId([u8; 16]);
pub struct DatabaseId([u8; 16]);
pub struct CapabilityId([u8; 16]);
pub struct ProvenanceId([u8; 16]);
pub struct EntityTypeId(NonZeroU32);
pub struct EventTypeId(NonZeroU32);
pub struct EnumTypeId(NonZeroU32);
pub struct EnumVariantId(NonZeroU32);
pub struct AggregateTypeId(NonZeroU32);
pub struct FieldId(NonZeroU32);
pub struct CommandId(NonZeroU32);
pub struct OutcomeId(NonZeroU32);
pub struct ProjectionId(NonZeroU32);
pub struct ProjectionGeneration(NonZeroU64);
pub struct IndexId(NonZeroU32);
pub struct InvariantId(NonZeroU32);
pub struct DigestKeyId(NonZeroU32);
pub struct EntityKey(pub Vec<u8>);
pub struct PartitionKey(pub Vec<u8>);
pub struct ConflictKey(pub Vec<u8>);
pub struct IndexEntryKey(pub Vec<u8>);
pub struct PartitionKeyHash(pub [u8; 32]);
pub struct ConflictKeyHash(pub [u8; 32]);
pub struct CanonicalInputHash(pub [u8; 32]);
pub struct ProjectionApplyHash(pub [u8; 32]);
pub struct IdempotencyKey(pub String);
```

`ProjectionIdentity` is the checked value tuple of `ContractLineage`, nonzero
`ProjectionId`, and `ProjectionPlanHash`. `FrontierPosition` is the closed
`BeforeFirst | AppliedThrough(CommitSequence)` representation shared by storage,
projection, and service. Projection group, prefix, frontier, and apply keys are
purpose-specific opaque newtypes; none is substitutable for an entity,
partition, conflict, or index key.

`RequestId` identifies one transport submission and is used for tracing. `IdempotencyKey` is a caller-selected command input used to recover an uncertain result. They are distinct values: retrying an uncertain command MUST use a fresh `RequestId` and MUST reuse the original `IdempotencyKey` and canonical command input.

ADR-0018 fixes UUIDv7 as 16 network-order bytes. `riffdb-types` provides only a
pure checked assembler from an unsigned 48-bit Unix-millisecond value and ten
random source bytes. Bytes `0..6` are the timestamp in big-endian order; byte 6
is `0x70 | (random[0] & 0x0f)`; byte 7 is `random[1]`; byte 8 is
`0x80 | (random[2] & 0x3f)`; and bytes `9..16` are `random[3..10]`. A timestamp
above `0xffff_ffff_ffff` rejects rather than clamping. The immutable golden for
timestamp `0x0123456789ab` and random bytes `00..09` is
`01234567-89ab-7001-8203-040506070809`.

System sources sample UTC once and fill exactly ten bytes through the approved
OS entropy provider. UUID time is non-authoritative metadata: clock rollback may
produce a lower identifier, and no UUID supplies transaction time,
authorization time, expiry, a deadline, idempotency input, or ordering.
Application and administration sequences remain the only authoritative orders.
The deterministic runtime, storage engines, and `riffdb-types` receive no clock
or entropy provider.

One fresh `RequestId` is created for each transport submission. Every retry uses
a new request ID while a pending admission preserves its original admission
request ID. Normal capability creation accepts a caller-supplied checked
`CapabilityId`; bootstrap generates and retains its capability ID and token
before transmission. A new database atomically installs a checked candidate
`DatabaseId` only when every initialization predicate proves the store is truly
uninitialized; reopen preserves the durable ID. The coordinator obtains a new
`ProvenanceId` only for a new commit attempt after successful evaluation and
before the authoritative transaction, never for reads or terminal replay.
Execution faults known before successful evaluation source none; the narrow late
transaction-current commit-check arithmetic path discards the one already-
sourced, never-persisted candidate and obtains no second candidate while
terminalizing. Unknown commit status is resolved through idempotency before
another provenance candidate is generated. Any durable-ID collision fails the
whole operation without overwrite or sequence assignment.

### Identifier requirements

- `ID-001` Request, agent-session, incident, database, capability, and provenance
  identifiers MUST be checked UUIDv7 values using ADR-0018's exact assembly and
  provider boundaries. No alternate identifier or monotonic ordering claim is
  accepted in the POC.
- `ID-002` `CommitSequence` MUST be a contiguous nonzero unsigned 64-bit integer
  on a single node, first assigned at 1. Zero is unassigned and exhaustion fails
  closed without wrap or reuse.
- `ID-003` `ContractVersion`, `DigestKeyId`, and every compiler-assigned numeric ID
  MUST be nonzero by construction and reject zero during decoding. Compiler IDs
  are allocated from 1, MUST be stable within a contract lineage, and MUST NOT be
  reused after removal; exhaustion fails closed without wrap.
- `ID-004` Durable keys MUST use canonical byte encodings independent of Rust memory layout.
- `ID-005` Secrets and raw capability tokens MUST never appear in logs, commit records, or metrics.

## 6.2 Transactional value system

POC transactional expressions support:

| Type | Notes |
|---|---|
| `bool` | Deterministic Boolean semantics |
| `i64`, `u64` | Checked arithmetic; overflow is a declared execution failure |
| `decimal<P,S>` | Fixed-point decimal; no IEEE floating point in transactional expressions |
| `money<CURRENCY>` | Fixed minor-unit representation with a declared currency |
| `string<N>` | UTF-8, compile-time maximum length |
| `bytes<N>` | Maximum length required |
| `timestamp` | UTC instant supplied by transaction context or input |
| `date` | Calendar date without timezone |
| `uuid` | 128-bit identifier |
| `enum` | Closed set in a contract version |
| `optional<T>` | Explicit nullability |
| `list<T,N>` | Bounded list only |

Public Protobuf decimal input may carry additive optional precision evidence.
When present it MUST equal the exact selected `decimal<P,S>` precision, while
scale always equals `S`; when absent the selected schema supplies precision.
Every new-server public decimal output carries its canonical checked precision.
This wire evidence never changes the canonical decimal or durable encoding.

`VAL-001` POC command evaluation MUST NOT support unbounded strings, bytes, collections, recursion, maps, or floating-point arithmetic.

`VAL-002` Arithmetic MUST be checked and deterministic across supported platforms.

`VAL-003` Durable record encoding MUST preserve unknown Protobuf fields where the chosen generated representation permits it, and schema evolution MUST never depend on Rust enum discriminants or struct field order.

## 6.3 Entity records

A durable entity record is a versioned value owned by a contract entity type.

```rust
pub struct EntityRecord {
    pub entity_type: EntityTypeId,
    pub key: EntityKey,
    pub entity_version: EntityVersion,
    pub written_by_contract: ContractVersion,
    pub fields: Vec<FieldValue>, // sorted by FieldId
}
```

- `ENT-001` Every entity record MUST carry a monotonically increasing entity version.
- `ENT-002` Field order in durable encoding MUST be canonical.
- `ENT-003` Unknown fields MUST remain readable during rolling contract evolution when compatibility rules allow it.
- `ENT-004` Entity state MUST only change through the commit coordinator.

---

# 7. Contract language v0.1

## 7.1 Design objective

The language is not a general-purpose programming language. It is a small, terminating, typed description of persistent entities, aggregate ownership, commands, invariants, typed mutations, durable events, queries, and simple projections.

The compiler MUST know every possible database read, mutation class, conflict key derivation, outcome variant, durable event type, and transactional source of nondeterminism.

## 7.2 End-to-end example

```text
contract LegalSpend version 1 {
  entity Budget {
    key (organization_id: uuid, fiscal_year: i64)
    field approved_amount: decimal<28,2>
    field allocated_amount: decimal<28,2>
    field updated_at: timestamp

    invariant non_negative:
      allocated_amount >= 0.00

    invariant within_approval:
      allocated_amount <= approved_amount
  }

  event BudgetAllocated {
    organization_id: uuid
    fiscal_year: i64
    matter_id: uuid
    amount: decimal<28,2>
  }

  aggregate AnnualBudget {
    root Budget
    partition_by organization_id
    conflict_key (organization_id, fiscal_year)
  }

  command CreateBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input approved_amount: decimal<28,2>

    idempotency_key idempotency_key
    create Budget(organization_id, fiscal_year) as budget
      else BudgetAlreadyExists {
        organization_id: organization_id,
        fiscal_year: fiscal_year
      }

    require positive_approval: approved_amount > 0.00
      else InvalidApprovedAmount { minimum: 0.01 }

    set budget.approved_amount = approved_amount
    set budget.allocated_amount = 0.00
    set budget.updated_at = tx.time

    return BudgetCreated { budget: budget }
  }

  command AllocateBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input matter_id: uuid
    input amount: decimal<28,2>

    idempotency_key idempotency_key
    mutate Budget(organization_id, fiscal_year) as budget
      else BudgetNotFound {
        organization_id: organization_id,
        fiscal_year: fiscal_year
      }

    require positive_amount: amount > 0.00
      else InvalidAmount { minimum: 0.01 }

    require sufficient_budget:
      budget.allocated_amount + amount <= budget.approved_amount
      else InsufficientBudget {
        approved: budget.approved_amount,
        allocated: budget.allocated_amount,
        requested: amount
      }

    set budget.allocated_amount = budget.allocated_amount + amount
    set budget.updated_at = tx.time

    emit BudgetAllocated {
      organization_id: organization_id,
      fiscal_year: fiscal_year,
      matter_id: matter_id,
      amount: amount
    }

    return Allocated {
      budget: budget,
      remaining: budget.approved_amount - budget.allocated_amount
    }
  }

  projection BudgetUtilizationDaily {
    source event BudgetAllocated
    key (organization_id, fiscal_year, tx.date)
    measure allocated = sum(amount)
    frontier transactionally_ordered
  }
}
```

## 7.3 Supported declarations

### Entities

An entity declares a stable name, primary key fields, typed fields, optional local indexes, and entity-local invariants.

### Aggregates

An aggregate declares:

- Root entity.
- Logical partition key.
- Mutation conflict key.
- Optional colocated child entities.
- Invariants that may span entities inside the same supported aggregate boundary.

### Commands

A command declares:

- Typed inputs.
- Exactly one idempotency key expression for mutating commands.
- Read, mutate, and create bindings with explicit absence/duplicate outcomes.
- Preconditions with explicit typed outcomes.
- Mutations.
- Durable events.
- Success outcome.
- Optional required capability scope and approval policy.

### Queries

POC queries support:

- Primary-key entity lookup.
- Bounded exact-prefix index scan.
- Projection lookup by complete or declared prefix key.
- Commit and provenance lookup through administrative APIs.

A query MUST declare a maximum result count. Unbounded scans are not part of the application contract language.

### Contract state machines (deferred)

Contract-authored state-machine source syntax, AST/HIR/IR nodes, transition
instructions, and runtime execution are outside POC grammar/IR version 1. The
POC command language mutates ordinary fields only through its compiled bounded
instructions, subject to declared predicates, invariants, postconditions, and
commit-time checks. This deferral does not permit generic writes or weaken
command-only mutation. Adding a contract state machine requires a future
accepted grammar and IR version with explicit dependency and compatibility
semantics.

### Durable events

Durable events are immutable typed payloads committed atomically with the command outcome. Event emission does not mean an external side effect has executed.

### Projections

POC projections are derived from durable event streams and support:

- Filter by equality and enum value.
- Key selection.
- Count.
- Sum of integer, decimal, or money values.
- Grouped count and sum.

Joins, distinct count, windows, arbitrary user functions, and approximation are unsupported in POC.

## 7.4 POC semantic restrictions

| ID | Rule |
|---|---|
| `DSL-001` | Command bodies MUST terminate and MUST NOT contain loops, recursion, dynamic dispatch, or host-language callbacks. |
| `DSL-002` | Mutating commands MUST declare an idempotency key. |
| `DSL-003` | All conflict keys MUST be computable from validated command inputs before any entity read. |
| `DSL-004` | A command MUST acquire every mutable aggregate capability before evaluating business preconditions. |
| `DSL-005` | Reads that influence a mutation MUST be represented in the plan's read dependency set. |
| `DSL-006` | Transactional code MUST NOT access the network, filesystem, environment, wall clock, process-global state, or OS randomness. |
| `DSL-007` | The only clock available in a command is `tx.time`, fixed for the admitted request. |
| `DSL-008` | The only effect operation is durable event emission. |
| `DSL-009` | Every precondition failure MUST map to a declared outcome variant. |
| `DSL-010` | Every command MUST have one terminal success outcome and MAY have multiple declared rejection outcomes. |
| `DSL-011` | Compiler and runtime MUST reject undeclared field access, mutation, event type, or outcome. |
| `DSL-012` | POC cross-partition mutation MUST fail compilation. |

Section 7.2 is the canonical v0.1 surface grammar. The POC grammar has one top-level `contract <Name> version <Integer> { ... }` declaration. It does not include modules, user-defined scalar aliases, or a separate `outcomes` block. Rejection variants are declared by binding or `require ... else ...` clauses, and the terminal success variant is declared by `return ...`. A future syntax extension requires an accepted language/IR ADR and compatibility fixtures; implementations MUST NOT accept alternate spellings merely because they appear in an old example.

## 7.5 Illustrative grammar excerpt

```ebnf
contract        = "contract" Ident "version" Integer "{" declaration* "}" ;
declaration     = entity | event | aggregate | command | projection ;
entity          = "entity" Ident "{" entity_item* "}" ;
entity_item     = key_decl | field_decl | invariant_decl | index_decl ;
aggregate       = "aggregate" Ident "{" aggregate_item* "}" ;
command         = "command" Ident "{" command_item* "}" ;
command_item    = input_decl | idempotency_decl | binding_decl
                | require_stmt | mutation_stmt | emit_stmt | return_stmt ;
binding_decl    = ("read" | "mutate" | "create") Ident "(" expr_list ")"
                  "as" Ident "else" outcome_expr ;
require_stmt    = "require" Ident ":" expr "else" outcome_expr ;
mutation_stmt   = "set" field_ref "=" expr ;
emit_stmt       = "emit" Ident object_expr ;
return_stmt     = "return" Ident object_expr ;
```

The complete grammar is an implementation artifact owned by `riffdb-contract-syntax`. The language reference generated from the grammar MUST be checked into the repository.

## 7.6 Outcomes and idempotency

The database distinguishes three categories:

1. **Declared outcome**: a business result such as `Allocated` or `InsufficientBudget`. It is not an infrastructure error.
2. **Execution failure**: authorization failure, malformed input, incompatible contract, storage failure, or internal defect.
3. **Client uncertainty**: the connection ended after submission and the caller does not know whether the command completed. The caller resolves this through idempotent retry or `GetOutcome`.

`OUT-001` A repeated idempotency key with the same command identity and canonical input hash MUST return the originally persisted typed outcome with `replayed=true`.

`OUT-002` A repeated idempotency identity with a different canonical input hash MUST return `IdempotencyKeyReuse` and MUST NOT execute the command. Reusing the same caller key for a different stable command ID is a distinct identity by construction.

`OUT-003` A business rejection MAY have zero entity mutations, but for an admitted idempotent command it MUST be persisted as a terminal command record so later retries return the same result.

A zero-mutation `OUT-003` result is still a commit-required command: every
influential snapshot dependency is revalidated and its declared outcome receives
the ordinary commit sequence, provenance, commit record, atomicity, replay, and
uncertainty semantics. It skips commit-check evaluation and index derivation
because it has no mutable post-images; current pre-images MUST NOT be substituted.
A nonzero result MUST instead provide exactly one complete mutation for every
mutable binding in the checked historical plan before complete commit-check and
index processing.

`OUT-004` The POC MUST NOT model duplicate delivery as a separate generic `Duplicate` business outcome. Replay is envelope metadata around the original outcome.

For the POC, idempotency identity is the tuple of database identity, environment, authorization-resolved tenant scope, stable principal ID, contract lineage, stable command ID, and a domain-separated keyed digest of the caller-supplied idempotency key. Contract version is stored with the reservation and outcome but excluded from lookup identity so an uncertainty retry survives active-version deployment. The identity never contains a raw capability token or raw idempotency key.

Before mutating evaluation, the commit coordinator durably creates or resolves a
pending idempotency reservation containing the identity, canonical input hash,
original admission request ID, exact `ExecutablePlanRef` (contract lineage,
contract version, contract bundle hash, command ID, and command plan hash), fixed
`tx.time`, admitted actor context, partition, and the separately stored approved
provenance-claim snapshot. A pending reservation receives no `CommitSequence`.
A retry with the same identity and input resumes or waits for that reservation and
reuses every frozen field; a retry with a different input fails without execution.
A terminal declared rejection receives one sequence like any other committed
declared outcome. Returning a persisted outcome on replay allocates no new
sequence.

For each committed declared outcome, the `idempotency` table contains exactly
one full `StoredOutcomeV1` `StoredEnvelope` under the canonical idempotency
identity. That envelope is simultaneously the terminal idempotency state and
the persisted outcome returned on replay; it is not a pointer to another outcome
row. The committing transaction deletes the corresponding
`StoredPendingAdmissionV1` row and installs this one outcome envelope atomically
with the commit record and the rest of the command record set. Pending deletion
writes no tombstone envelope, and no second terminal or outcome envelope exists.

ADR-0012 adds a closed terminal admission state
`ExecutionFailed { ArithmeticFault | ResourceLimit }`. It may be persisted only
after every influential entity absence/version and index-range epoch from the
evaluation snapshot is equal in transaction-current state. Changed evidence
causes bounded full reevaluation or leaves the admission pending. This state
consumes the idempotency identity but creates no declared outcome, application
commit, `CommitSequence`, event, outbox intent, or command provenance. A proven
abort leaves the admission pending and maps to storage unavailability; unknown
commit status fences writes and maps to outcome uncertainty until same-key
resolution.

ADR-0023 fixes bounded full reevaluation at three attempt slots per outer
invocation, including the initial materialization. A valid lineage-expansion
resource overflow consumes its slot without entering runtime. Exhausting that
fixed invocation-local budget leaves the durable Pending admission byte-for-byte
unchanged and creates no terminal record or application sequence. The service
maps the coordinator's internal `RetryBudgetExhausted` to the existing public
`ConcurrencyDeadlineExceeded`; a later caller retry is a new bounded invocation
under the same idempotency identity and frozen admission.

An impossible validated-plan/snapshot `Integrity` fault is a redacted internal
incident, leaves a mutating admission pending for operator intervention, and never
uses the expected `ExecutionFailed` transition. The POC supplies no online repair
path; recovery remains fail-closed.

Grammar-v1 read-only commands are unjournaled. They create no pending or terminal
command-idempotency record, persisted outcome, provenance, application commit, or
`CommitSequence`. Their optional request-correlation key is transport metadata
only, and required service audit is a separate outcome-free administration
record. Durable read-only replay or journaling requires a future accepted ADR.

## 7.7 Contract compatibility

The compiler MUST assign stable numeric IDs to entities, fields, commands, outcomes, and events. A contract bundle records lineage and compatibility metadata.

### POC-compatible changes

- Add a new command.
- Add a new query.
- Add an optional field with a deterministic default.
- Add a new event type.
- Add a new projection.
- Add a new outcome to a command only when callers explicitly opt into the new contract version.

### POC-incompatible changes

- Change field type or key encoding.
- Remove or reuse a stable numeric ID.
- Change a command's idempotency expression.
- Change conflict-key derivation.
- Narrow an invariant without a migration plan.
- Change outcome payload types in place.
- Change an event's existing field meaning.
- Remove an entity, field, command, outcome, event, or projection.

The POC MAY require a stop-the-world contract deployment. The MVP MUST define rolling compatibility between server nodes, SDKs, and active contract versions.

---

# 8. Compiler and contract bundle

## 8.1 Compilation pipeline

```text
source files
  -> Logos tokens with spans
  -> LALRPOP syntax AST
  -> name resolution
  -> typed HIR
  -> invariant classification
  -> command dependency analysis
  -> locality and conflict analysis
  -> deterministic executable IR
  -> JSON input/output schemas
  -> contract bundle + plan hash
```

## 8.2 Required compiler passes

| Pass | Output | Required diagnostics |
|---|---|---|
| Parse | Syntax AST and source map | Unexpected token, incomplete declaration, duplicate syntax item |
| Resolve | Symbol table and resolved references | Unknown type/entity/field/event/outcome, duplicate name |
| Type check | Typed HIR | Type mismatch, nullability, arithmetic overflow potential, invalid comparison |
| Shape validation | Stable IDs and key layouts | Unsupported key type, unbounded value, incompatible schema change |
| Dependency analysis | Read set templates and field dependencies | Hidden read, undeclared binding, unbounded scan |
| Conflict analysis | Canonical conflict-key expression | Key not input-computable, missing mutable capability, cross-partition mutation |
| Invariant classification | Supported invariant plans | Unsupported global predicate, unsupported aggregation, cyclic dependency |
| Outcome analysis | Exhaustive typed result schema | Missing success, undeclared rejection, duplicate outcome field |
| Projection analysis | Incremental operator plan | Unsupported join/window/distinct, unbounded state |
| Code generation | Executable IR, schemas, docs | Internal compiler consistency errors |

## 8.3 Diagnostic requirements

- `CMP-010` Every user-facing compiler error MUST include a stable code, concise summary, source span, and actionable help when possible.
- `CMP-011` Diagnostics MUST be snapshot-tested.
- `CMP-012` Internal panics on user input are defects; compiler entry points MUST return structured diagnostics.
- `CMP-013` `riffdb contract explain` MUST show derived partition key, conflict keys, reads, writes, invariants, events, outcomes, and estimated execution class.

Example:

```text
error[ADB-C042]: annual budget invariant is not protected by the declared conflict key
  --> legal_spend.adb:27:5
   |
27 |     conflict_key matter_id
   |     ^^^^^^^^^^^^^^^^^^^^^^ commands for different matters may update the same annual budget
   |
help: use conflict_key (organization_id, fiscal_year)
```

## 8.4 Executable command plan

```rust
pub struct CommandPlan {
    pub command_id: CommandId,
    pub name: String,
    pub contract_version: ContractVersion,
    pub plan_hash: PlanHash,
    pub input_schema: JsonSchema,
    pub output_schema: JsonSchema,
    pub partition_expr: ExprId,
    pub conflict_key_exprs: Vec<ExprId>,
    pub read_bindings: Vec<ReadBindingPlan>,
    pub invariants: Vec<InvariantPlan>,
    pub instructions: Vec<Instruction>,
    pub retry_policy: RetryPolicy,
    pub required_capability: CapabilityRequirement,
}
```

The POC interpreter executes a typed, forward-only instruction stream. Branch targets MUST be validated during compilation. The runtime MUST NOT evaluate source AST nodes.

Illustrative instructions:

```rust
pub enum Instruction {
    LoadInput { input: InputId },
    ReadEntity { binding: BindingId, key_expr: ExprId },
    Require { predicate: ExprId, reject_outcome: OutcomeExprId },
    SetField { binding: BindingId, field: FieldId, value: ExprId },
    EmitEvent { event: EventTypeId, payload: ObjectExprId },
    Return { outcome: OutcomeExprId },
}
```

## 8.5 Contract bundle

```rust
pub struct ContractBundle {
    pub format_version: u32,
    pub contract_name: String,
    pub contract_version: ContractVersion,
    pub parent_version: Option<ContractVersion>,
    pub source_hash: [u8; 32],
    pub plan_hash: PlanHash,
    pub schema: SchemaIr,
    pub commands: Vec<CommandPlan>,
    pub queries: Vec<QueryPlan>,
    pub projections: Vec<ProjectionPlan>,
    pub mcp_command_names: McpCommandNameRegistryV1,
    pub compatibility: CompatibilityReport,
    pub compiler_version: String,
    pub ir_format_version: u32,
}
```

`McpCommandNameRegistryV1` is the checked, versioned ADR-0020 registry. It binds
the exact contract lineage and source contract identifier, and contains one
entry per executable command with its nonzero `CommandId`, exact source command
identifier, and complete `McpCommandToolNameV1`. Entries are ordered by
`CommandId`; missing, duplicate, reordered, misbound, noncanonical,
over-length, or colliding entries reject. The registry is part of canonical
bundle serialization and hashing. The exact Rust representation remains owned
by `riffdb-contract-ir`; this illustrative bundle shape does not authorize an
adapter-owned string registry or a second normalization path.

`CMP-020` Bundle serialization MUST be deterministic. Compiling identical source with the same compiler version MUST produce byte-identical bundles and the same plan hash. Wall-clock compilation or deployment time MUST NOT appear in a bundle.

`CMP-021` The bundle MUST record compiler version and IR format version separately from application contract version.

`CMP-022` The server MUST reject a bundle whose IR version it cannot execute.

### Bounded active-lineage materialization proof

`riffdb-catalog` owns these fixed v1 limits:

```text
MAX_ACTIVE_LINEAGE_BUNDLES_V1 = 4_096
MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1 = 64 * 1024 * 1024 = 67_108_864
MAX_LINEAGE_MATERIALIZATION_PROOF_BYTES_V1 = 2 * 1024 * 1024 = 2_097_152
```

The active lineage is the exact inclusive chain from genesis through the active
bundle. Its canonical-byte charge is a checked sum of each member's exact
`ContractBundle::canonical_bytes().len()` and excludes catalog envelopes, keys,
evidence-page framing, and allocator overhead. These aggregate limits are
independent of the existing 15 MiB per-bundle limit. The 4,096 active-lineage
limit is intentionally lower than the generic 65,535-bundle backup-table bound,
and the 64 MiB lineage charge is distinct from both the 15 MiB bundle and 16 MiB
historical-evidence page limits.

Both `ActiveCatalogSnapshot::read` and `resolve_executable_plan` MUST load the
current active pointer and walk from the active bundle to genesis through each
exact parent `(ContractVersion, ContractBundleHash)`. They reverse that walk
into genesis-to-active order, reject gaps, cycles, repeated versions, hash
substitution, lineage mismatch, unsupported versions, or either aggregate-limit
excess, and re-run the same pure forward compatibility comparator used for
activation on every adjacent pair. Numeric `ContractVersion` comparison is not
ancestry evidence. The referenced executing bundle and plan must be an exact
member of that chain, which permits a pending historical plan to read records
written by its validated ancestors or descendants without accepting a foreign
lineage.

Successful resolution constructs one catalog-owned
`LineageMaterializationProofV1`, shared process-locally by `Arc` and attached to
the `ResolvedExecutablePlan`. The proof has private construction, is not
serializable, is not a durable or public DTO, never crosses a storage trait, and
is excluded from canonical bundle bytes, IR, every hash, and every Protobuf
message. For accounting and reproducible tests, its exact charged framing is:

```text
u8 proof_version (= 1)
u32 lineage_len || lineage
u32 bundle_count
repeated(u64 ContractVersion || [u8; 32] BundleHash)
u16 executing_bundle_ordinal
u32 owner_count
repeated(
  u8 owner_tag { entity = 1, event = 2 }
  || u32 owner_id
  || u32 field_count
  || repeated(u32 FieldId || u16 introduced_at_ordinal)
)
```

All integers use big-endian byte order. Bundle and introduction ordinals are
zero-based and at most 4,095; `BundleHash` is the exact 32-byte
`ContractBundleHash`. Bundle entries are in genesis-to-active order;
owners are duplicate-free in `(owner_tag, owner_id)` order; fields are
duplicate-free in ascending `FieldId` order. At most 8,192 entity/event owners
and the existing 262,144 lineage-ledger field entries are charged. The lineage
is bounded at 256 bytes. Checked arithmetic and compile-time assertions MUST
retain this exact maximum proof charge:

```text
1 + (4 + 256) + 4 + (4_096 * 40) + 2 + 4
  + (8_192 * 9) + (262_144 * 6)
= 1_810_703 bytes
```

That is 286,449 bytes below the 2 MiB cap. Any change to a constituent bound
that invalidates the assertion requires human review before merge. The catalog
derives every field's first introduction ordinal from the revalidated parent and
child schemas; it MUST NOT trust a stored compatibility report as sufficient
evidence. Boundary tests exercise the checked proof-charge calculator
synthetically at exactly 2 MiB and one byte above it; they separately construct
or calculate the valid-v1 1,810,703-byte maximum and MUST NOT claim a valid v1
proof can reach the 2 MiB cap.

For one exact stored writer bundle and one executing descendant schema, the
proof yields an opaque null-fill mask over the executing schema's fields in
ascending `FieldId` order. The mask is exactly `ceil(field_count / 8)` bytes,
has zero unused high bits, and is at most 512 bytes under the existing 4,096
fields-per-owner limit. Across the existing maximum 4,096 combined binding and
aggregate-root positions, transient masks therefore have an exact independent
structural maximum of 2 MiB. That maximum is not an additional snapshot budget:
the retained normalized `ReadSnapshot` semantic bytes plus every retained
nonempty bitset payload byte MUST together remain at or below the existing 16
MiB command-snapshot ceiling. Binding/root and mask vectors are aligned, so the
position is implicit and adds no separate metadata charge. Tests freeze exact
combined acceptance at 16 MiB and rejection at one byte more. Masks are
process-local normalization authority, not serialized or hashed data.

`riffdb-catalog` owns the opaque process-local command-materialization evidence
API bound to the exact `ResolvedExecutablePlan`. A successful first pass returns
`Ready`: the bounded normalized snapshot plus opaque current-recheck evidence.
A valid proof-authorized expansion beyond the ceiling instead returns resource
evidence retaining only the original bounded raw `ReadSnapshot` together with
the exact resolved plan/proof. Returned resource evidence MUST retain no
over-budget normalized record or mask. The catalog-owned pure current-recheck
operation accepts transaction-current raw observations only after the
coordinator has compared dependencies; it proves exact raw-observation equality
and deterministically repeats the same normalization and exact charge. It
performs no catalog lookup, storage I/O, clock, entropy, or runtime evaluation.
The exact result and charge are semantic; internal allocation strategy,
transient masks, and evaluation order are not frozen. These opaque values add no
durable or protocol field and never cross a storage trait.

A missing field may be materialized as `CanonicalValue::Null` if and only if
the stored writer is an exact chain member,
`writer_ordinal < field_introduction_ordinal <= executing_ordinal`, and the
executing field is optional-with-null-default. A missing field written by the
executing bundle itself, by a descendant of it, or introduced in genesis is an
integrity failure, as is a missing required field, malformed mask, or foreign
lineage/version/hash. Present fields unknown to the executing historical schema
remain attached to the canonical record unchanged but invisible to its
expressions. Normalization emits fields in canonical `FieldId` order and never
drops or rewrites an unknown value.

Projection consumption uses the same catalog-proved omission authority without
rewriting an authoritative event. `riffdb-catalog` resolves the exact projection
bundle and plan, combines them with the enclosing commit's exact
`ExecutablePlanRef` and immutable durable event, and returns an opaque
process-local event-materialization view. The view is nonserializable,
non-durable, non-Protobuf, and forbidden across a storage trait. It retains
unknown payload fields unchanged but exposes only fields known to the exact
resolved projection plan to projection expressions. It may add nulls only for
strict-ancestor optional-with-null-default fields under the rule above. A
foreign writer, or an exact, descendant, genesis, or required-field omission,
fails closed; a descendant event with no such omission may retain fields unknown
to the resolved plan. The original event payload, canonical bytes, `EventHash`,
and stored copies remain byte-for-byte immutable, and storage never interprets
the event schema or contract IR.

Activation preparation computes `current_count + 1`,
`current_canonical_bytes + candidate.canonical_bytes().len()`, and the rebuilt
proof charge with checked arithmetic against the exact currently active chain.
It rejects a count excess as one root `ValidationCode::TooManyItems` issue and a
canonical-byte or proof-byte excess as one root `ValidationCode::TooLong` issue;
both are public `Validation` errors with `CorrectRequest`, occur before
coordinator submission, and retain ordinary authenticated service-audit
behavior. The coordinator's expected-active comparison remains the authority
for a concurrent activation race. A catalog storage read still returns
`CatalogError::Storage` rather than being reclassified as validation.

Startup rebuilds the same exact chain and proof and enforces the same three
ceilings. An already-active history with a broken chain or excess count,
canonical bytes, or proof bytes is `InvalidHistoricalEvidence`, leaves
authoritative readiness false, and surfaces only a redacted `InternalDefect`.
Once activation has admitted a chain, reaching any proof or aggregate ceiling
inside command resolution or normalization is an internal defect, never a
caller-controlled `ResourceLimit`.

During production open, `riffdb-catalog` is the sole IR-aware validator of
historical contract semantics. The server composition feeds it the bounded
`HistoricalSemanticEvidence` pages obtained from one storage structural-evidence
session, in order, through the exact end marker. The catalog resolves every
referenced immutable bundle and executable plan, validates the recorded lineage,
version, IR version, plan hash, and active-pointer relationship, and revalidates
the complete ADR-0020 command-name registry without repair. Only exact-end
completion may produce opaque catalog-owned `ValidatedCatalogHistory` bound to
the same database/open session as the evidence. A skipped,
repeated, reordered, truncated, unknown, mismatched, or cross-session page fails
closed.

That proof is process-local startup authority, not durable data or a storage DTO.
It never crosses a storage trait and storage never imports, decodes, or interprets
contract IR. The boundary accepts neither a generic storage validation callback
nor a catalog callback invoked from inside an engine transaction.

## 8.6 Deployment

1. Validate source and compile bundle without mutation.
2. Compare with active contract and produce a compatibility report.
3. Run static policy checks.
4. Optionally execute replay tests against a branch or fixture dataset.
5. Submit deployment with expected active contract version.
6. Atomically persist the bundle and active-version pointer.
7. Refresh generated schemas and MCP tool catalog.
8. Emit contract and resource list-change notifications.

Deployment time, approving principal, request ID, and other operational metadata belong in the ordered catalog administration audit record, not in `ContractBundle`.

The POC supports only one active contract version for new commands. Historical commit records retain their original contract version and plan hash.

Contract deployment and active-version changes are authoritative control-plane mutations. They MUST be submitted as typed administrative operations through the same commit coordinator used for commands; `riffdb-catalog`, API adapters, and storage implementations MUST NOT update catalog state directly. These operations produce a separately ordered administration audit record rather than an application `CommitSequence` unless a future accepted ADR deliberately generalizes the commit-record model.

---
# 9. Transaction and command execution semantics

## 9.1 Request lifecycle

[Graphviz source — command execution sequence](diagrams/command_sequence.dot)

A mutating command follows this sequence:

1. The adapter decodes and bounds the transport request, extracts one opaque
   credential, calls `CredentialAuthenticator` with trusted database,
   environment, and audience context, and constructs a checked `RequestContext`.
2. The service inspects the idempotency state without creating a reservation,
   selects the active/explicit plan for an absent identity or the exact stored
   historical plan for an existing identity, loads that checked plan, and only
   then knows whether a generic execute request is mutating or read-only. An
   unknown target before this classification is bounded security telemetry, not
   a fabricated mutating invocation.
3. The service validates and canonicalizes input under that exact schema,
   computes its hash, and uses the shared pure `riffdb-invariant` evaluator to
   derive the one `PartitionKey`, all mutation conflict keys, and authorization
   facts. Checked arithmetic failure in this pre-admission, input-only phase is
   root `ValidationCode::OutOfRange`: it creates no pending admission and can
   never become `ExecutionFailed`. A checked plan/input combination that cannot
   be evaluated as its validated type declares is an internal integrity failure,
   not an arithmetic validation result. Arithmetic after snapshot evaluation
   uses ADR-0012's dependency-validated terminalization as narrowed for lineage
   expansion in steps 10 and 13 below.
4. The service performs an initial current-policy check over the exact facts and
   validates only allowed provenance claims. Every explicit authenticated policy
   denial appends one standalone `denied` record before returning denial.
5. For an allowed mutation, the service durably appends `started`, then waits for
   a bounded executor-capacity permit. Failure or cancellation before submission
   appends the matching terminal phase and admits no command.
6. With the permit held, the service reloads current policy and samples a fresh
   authorization time for the same exact facts. A denial appends terminal
   `denied`; a clock/internal failure appends `failed`. On allow, the service
   synchronously consumes the permit to submit one privately authorized typed
   preparation. This final check immediately followed by executor acceptance is
   the non-retroactive command-authorization boundary. The service never
   constructs an `EvaluatedCommand` or `CommitIntent` and never receives a
   storage write handle.
7. The coordinator rechecks the inspected identity, then atomically creates a
   pending admission, resumes an equal pending admission, returns an equal
   terminal replay, or rejects mismatched input. A concurrent change may request
   at most the ADR-0007 bounded inspect/confirm retry.
8. For a new admission, the coordinator freezes the original request ID, exact
   `ExecutablePlanRef`, canonical input hash, logical time, admitted actor,
   partition, and approved provenance claims. Resume never substitutes current
   request claims or the active plan.
9. The coordinator derives, sorts, deduplicates, and acquires every declared
   mutation `ConflictKey`, then rechecks the pending admission. Dynamic
   acquisition, upgrade, and cross-partition mutation are forbidden.
10. A synchronous storage read copies every declared binding and range
    observation into one owned bounded `ReadSnapshot`, closes the engine read
    view, and returns canonical read dependencies. After the view is closed, the
    coordinator consumes one invocation-local attempt slot and invokes the
    catalog-owned opaque materialization API bound to the resolved plan. `Ready`
    supplies the bounded normalized snapshot and current-recheck evidence;
    eligible ancestor-written
    optional omissions are canonical null and unknown fields remain byte-for-
    byte present. A proof failure is integrity. Valid expansion beyond the fixed
    execution budget returns dependency-sensitive resource evidence retaining
    only the raw bounded snapshot plus the exact resolved plan/proof and no
    over-budget normalized data or masks.
11. The deterministic runtime evaluates the exact checked plan, normalized
    snapshot, immutable `TransactionContext`, and fixed `EvaluationBudget`, with
    no storage transaction, I/O, clock, entropy, provenance claims, or `.await`.
    It rejects a remaining missing declared field as integrity and performs no
    blind optional-field fill.
12. A successful mutating evaluation returns storage-owned `EvaluatedCommand`.
    For a new commit attempt, the coordinator obtains one `ProvenanceId` from its
    injected source and combines it with the exact stored admission and plan-
    derived partition/conflict evidence into the final self-contained
    `CommitIntent`. Replay never invokes the source. An `ExecutionFailed` path
    invokes no source unless transaction-current commit-check arithmetic fails
    after successful evaluation has already sourced one candidate; that narrow
    late-fault path discards the never-persisted candidate and sources no second
    one while terminalizing.
13. An arithmetic or resource fault follows ADR-0012's dependency-validated
    `Pending -> ExecutionFailed` transition; changed evidence triggers full
    reevaluation or leaves the admission pending, and no application sequence is
    assigned. For a lineage-expansion resource fault, terminalization first
    compares every dependency, then invokes the resource evidence's catalog-
    owned pure current-recheck operation. That operation requires exact equality
    of every raw physical binding/root observation and deterministically
    re-derives the same over-limit result before the coordinator may write
    `ExecutionFailed`. A proven pre-commit abort ends that attempt; a
    later safe
    reevaluation of the same Pending admission is a new attempt and may source
    one new provenance candidate. Unknown status is resolved against the same
    idempotency identity before any reevaluation or source call.
14. For a commit-required result, the coordinator opens a short synchronous
    write transaction and starts a count-only candidate. Starting the candidate
    checks only the 64-command count ceiling; it does not assign a sequence or
    guess an encoded write-set charge.
15. The transaction rechecks the exact pending identity, input, admission, and
    plan reference, then reads all influential validation targets from
    transaction-current state and compares every absence/version/epoch
    dependency regardless of mutation count. `DependencyChanged` aborts and
    requests reevaluation before any transaction-current normalization. For each
    equal present dependency, the coordinator passes the raw current observation
    to the `Ready` evidence's catalog-owned pure current-recheck operation. That
    opaque operation proves exact equality with the retained raw pre-runtime
    observation across target, entity version, writer identity, schema binding,
    canonical field order, and every physical field, then returns the normalized
    current record for value-source or commit-check use. Drift behind an equal
    version is integrity. It performs no catalog lookup or extra storage I/O in
    the transaction. A missing field not proved eligible
    for ancestor null fill is integrity, and unknown fields remain present. A
    zero-mutation declared business outcome then skips commit-check evaluation
    and index derivation. A nonzero mutation set must first prove exactly one
    complete mutation for every
    mutable historical-plan binding, then evaluates the complete commit-check
    plan over normalized current read/root values plus those proposed complete
    post-images. A missing post-image is never replaced by a current pre-image.
16. Only for a nonzero mutation set, after semantic validation the coordinator
    derives the canonical set of mutation-affected index-prefix epoch targets
    from transaction-current old entries and proposed new entries. Storage reads
    those epoch positions in the same write transaction. A zero-mutation outcome
    retains empty index deltas and affected-epoch targets. Mutation-affected
    targets are distinct from influential range-epoch read dependencies, except
    when one exact prefix happens to belong to both sets.
17. The coordinator freezes the exact sequence-free `CommandWriteSetPlanV1` and
    reserves its semantic charge plus conservative per-record-class and
    aggregate canonical `StoredEnvelope` capacity. This exact aggregate staged-
    write reservation occurs inside the transaction, after current reads and
    before sequence assignment.
18. Only after that reservation succeeds may the coordinator assign the next
    nonzero `CommitSequence`, construct the exact complete record graph, verify
    that it matches the retained intent, write plan, and assignment, and prove
    each actual canonical `StoredEnvelope` byte length does not exceed its
    reserved per-class upper bound before staging.
19. The storage commit atomically deletes the pending row and installs exactly
    one full `StoredOutcomeV1` envelope as both terminal idempotency state and
    persisted outcome together with sequence metadata, entity/index changes,
    events/outbox intent, provenance, and the commit record, or makes none of
    them durable. It writes no pending tombstone or second terminal envelope.
20. The coordinator releases logical capabilities and publishes the exact
    first-durable application `CommitSequence` through its injected
    least-authority notification sink. Replay, read-only, failed, audit,
    initialization, and control-plane paths publish nothing. A sink error or
    panic preserves the already-known committed result for its current caller
    but stops coordinator admission and process readiness.
21. The service applies every current disclosure obligation and constructs the
    fully bounded, filtered, redacted semantic result. A shaping failure is a
    failure and never produces a false successful audit.
22. The service appends the matching terminal service-audit record, then releases
    the already-safe typed result. Failure to persist the required terminal audit
    withholds the result and uses the accepted uncertainty/unavailable recovery
    path without rolling back authoritative state.

Grammar-v1 read-only execution uses the same adapter, service validation,
current authorization, exact checked plan, owned snapshot, and
deterministic runtime. It uses the runtime `ReadOnly` result and creates no
pending/terminal command journal, persisted outcome, command provenance, or
application sequence. Any policy-required service audit remains a separate
administration record and contains no outcome or execution-fault detail; an
ordinary allowed standard read without that obligation is unaudited.

## 9.2 Transaction context

```rust
pub struct TransactionContext {
    pub request_id: RequestId,
    pub actor: AdmittedActorContext,
    pub plan: ExecutablePlanRef,
    pub tx_time: LogicalTime,
    pub partition_key: PartitionKey,
}
```

For a mutating command, `request_id` is the original admission request ID frozen
in the pending record; a retry's outer request ID is audit/trace context only.
For an unjournaled read-only command it is the checked current invocation ID.
`AdmittedActorContext` contains only stable principal ID, trusted actor kind,
authorization-resolved tenant scope, and optional admitted agent-session ID.
Approved source repository, source commit, reason, and approval are stored
separately as `StoredAdmittedProvenanceClaimsV1` and never enter runtime.

The coordinator reads its injected `AdmissionClock` exactly once before a new
admission, validates the accepted signed-seconds/nanoseconds timestamp, and
stores the resulting `LogicalTime`. Resume and replay do not read the clock.
An unjournaled read-only invocation receives one invocation-local logical time
with no cross-invocation replay promise. Logical time is not commit order and is
never clamped or used for authorization expiry, leases, keys, or uniqueness.

`riffdb-commit` owns the synchronous `AdmissionClock` consumer port and the
separate synchronous `AdministrationClock` used for coordinator-observed
catalog/bootstrap/service-audit timestamps. `riffdb-server` injects the concrete
OS provider. Authentication and current-policy authorization retain their own
auth- and policy-owned clock ports. None is substitutable in a semantic API, and
none enters deterministic runtime. Clock timestamps need only be canonical;
sequences, not wall time, determine authoritative order.

Runtime also receives an immutable plan-derived `EvaluationBudget`. The v1
non-runtime reserve inside the 15 MiB semantic `CommitIntent` ceiling is exactly
65,536 bytes, so `EvaluatedCommand` semantic content is capped at exactly
15,663,104 bytes. Admission rejects before evaluation when its fixed fields
cannot fit the reserve. The budget comes only from accepted IR/storage hard
limits, is equal for equal plan/format versions, and cannot be raised, lowered,
or selected by an adapter, request, provenance claim, or process configuration.

- `TXN-010` `tx.time` MUST be fixed before command evaluation and reused for retries of the same admitted idempotency record.
- `TXN-011` POC commands MUST NOT have access to randomness. Deterministic recorded randomness MAY be introduced by MVP ADR.
- `TXN-012` Runtime evaluation MUST be synchronous and free of `.await` points after mutable capabilities are acquired.
- `TXN-013` Mutable capabilities MUST not be serializable, cloneable into user code, or valid after terminal execution.

## 9.3 Conflict manager

A conflict key is an opaque canonical byte string with a type prefix and aggregate identity. Example:

```text
0x43 | 0x01 | aggregate_type_id:u32_be | organization_uuid:16 | fiscal_year:sign_flipped_i64_be
```

### POC lock semantics

- Exclusive mutation capability only; no lock upgrades.
- All keys are known before acquisition.
- Keys are sorted and deduplicated before waiting.
- Wait queues are FIFO per key.
- A multi-key waiter is granted only when all requested keys can be granted.
- Cancellation removes the waiter if no commit has begun.
- Lock timeout returns the transient public `ConcurrencyDeadlineExceeded`
  failure, not a declared business outcome or ADR-0012 `ExecutionFault`. It does
  not terminalize or consume the idempotency admission.
- ADR-0023's distinct internal full-reevaluation budget exhaustion maps to the
  same existing public `ConcurrencyDeadlineExceeded` code, safe message, retry
  action, and gRPC `DEADLINE_EXCEEDED` status without becoming a lock result.

A grammar-v1 command may observe multiple logical conflict domains only within
its one statically proven `PartitionKey`. Every domain it may mutate and every
corresponding conflict key is derived and acquired before evaluation; every
influential observation outside those mutation domains is represented by
canonical dependency evidence and revalidated. Cross-partition mutation,
dynamic acquisition, and capability upgrade are rejected.

```rust
pub trait ConflictManager: Send + Sync {
    async fn acquire_mut(
        &self,
        keys: Vec<ConflictKey>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MutationLease, ConflictError>;
}
```

`TXN-020` The lock table's internal mutex MUST never be held across `.await`.

`TXN-021` Lease release MUST be idempotent and guaranteed on normal return, error, panic containment, and task cancellation.

`TXN-022` The conflict manager MUST emit lock-wait duration and conflict-key hash
metrics without exposing raw business keys. The deterministic runtime never
observes lock waits or conflict-manager internals.

`TXN-023` Loom tests MUST cover grant, release, cancellation, timeout, fairness, and multi-key ordering for a reduced lock manager model.

## 9.4 Read dependencies

The closed v1 dependency registry is:

```text
EntityObservation
  entity type + complete EntityKey
  expected = Absent | Present(nonzero EntityVersion)

IndexRangeEpoch
  IndexId + validated component-complete prefix
  + IndexEpochPosition::{BeforeFirst, Value(nonzero IndexEpoch)}
```

Every binding observation contributes an entity dependency, including observed
absence. Every influential accepted range contributes one epoch dependency.
Dependencies are canonically ordered and duplicate-free; conflicting duplicate
observations are an integrity defect. A range epoch is read from the same
snapshot as its entries and advances atomically for every affected whole-index
and complete leading-component prefix bucket when an index entry or covered
value changes.

Grammar and executable IR version 1 declare index key components but no covering
fields. Every coordinator-produced v1 index entry therefore carries the one
canonical empty `CanonicalRecord` as `covered_values`; the coordinator MUST NOT
copy an entity, indexed fields, or an ad hoc projection into that field. The
storage semantic API and durable codec remain generic and retain bounded covered
values plus their epoch behavior. Any nonempty v1 producer requires a future
accepted language/IR and durable compatibility decision.

Checked command-plan construction conservatively computes the maximum successful
grammar/IR-v1 index shape before hashing or activation. It rejects a mutating
plan if that shape can exceed 4,096 index-entry mutations, 4,096 distinct
mutation-affected prefix targets, 4,096 combined binding, root-validation, and
affected-prefix validation positions, or the 16 MiB affected-target/current-
epoch-state bound. The estimator may conservatively reject a shape whose actual
runtime values would deduplicate across bindings; this is a permitted lower
compiler acceptance limit, not a relaxation of a storage ceiling. It adds no IR
field, encoding tag, durable value, or plan-hash input. Runtime derivation and
storage constructors retain incremental count and byte guards as defense in
depth; a canonical checked plan reaching one is an internal integrity defect,
never a durable or public command `ResourceLimit` outcome.

An influential `IndexRangeEpoch` dependency and a mutation-affected epoch target
serve different purposes and MUST NOT be inferred from one another. The first is
snapshot evidence for a range whose contents influenced evaluation and is
compared during transaction-current dependency validation. The second is the
canonical union of whole-index and complete leading-prefix buckets affected by
transaction-current old index entries and proposed new entries. It is derived
only after private validation, read in that same write transaction, and advanced
once by the staged command. An exact prefix may appear in both sets, but equality
of the sets is neither required nor generally correct.

Captured predicate booleans or values are not commit proof and are not a v1
storage dependency. Predicate and invariant correctness uses the exact
historical `ExecutablePlanRef`, a structurally checked validation request, and a
coordinator-private semantic match. Every influential dependency is compared for
both zero- and nonzero-mutation outcomes. A nonzero mutation set must exactly
cover every mutable plan binding before the complete commit-check plan is
re-evaluated over transaction-current read/root values and proposed post-images.
A zero-mutation declared outcome skips commit-check and index derivation without
falling back to mutable pre-images.

Dependency comparison precedes transaction-current normalization. Any changed
absence, entity version, or range epoch aborts the candidate and requests full
reevaluation. When a present entity version is equal, the coordinator invokes
the catalog-owned opaque current-recheck operation on the retained `Ready` or
resource evidence and the raw current observation. That pure operation requires
exact raw-record equality with the retained pre-runtime observation, including
target, entity version, writer identity, schema binding, canonical field order,
and every physical known or unknown field/value, before deterministic
normalization or overflow reproduction. Unequal content behind an equal version
is `InternalDefect`, not `DependencyChanged`; no catalog lookup, storage
callback, or second read is allowed to explain or repair it inside the write
transaction.

Grammar v1 does not enable write-influencing indexed range reads. A future
bounded indexed command-read IR requires an accepted static-target/epoch policy
and any required exclusion must use explicit conflict keys known before
evaluation. The storage epoch capability and conformance tests do not expose a
hidden runtime scan.

## 9.5 Commit intent

`riffdb-storage-api` owns both `EvaluatedCommand` and the transport-neutral
`CommitIntent`. Runtime constructs only `EvaluatedCommand`, containing the exact
plan reference, canonical binding/range targets and dependencies, complete
mutation post-images with expected observations when mutations are present,
ordered pre-commit event values, and declared encoded outcome. An admitted
mutating command may carry zero mutations for a declared business rejection
under `OUT-003`; otherwise it must carry exactly one complete mutation for every
mutable plan binding. It contains no admission identity, actor, provenance,
logical time, sequence, event ID, durable record, or storage handle.

Every present entity/root record consumed to produce an `EvaluatedCommand` has
already passed catalog-proof-guided normalization. Each inserted field adds
exactly six canonical bytes: four bytes of `FieldId` plus the canonical
value-version and null-tag bytes. The existing 1 MiB per-record, 16 MiB owned-
snapshot, and fixed evaluation limits are rechecked with checked arithmetic
during expansion. The snapshot charge includes retained normalized semantic
bytes plus every retained nonempty null-mask bitset payload byte; aligned
binding/root and mask vectors make position implicit with no separate metadata
charge. The mask's 2 MiB structural maximum grants no extra capacity. If a valid
proof-authorized expansion exceeds one of those limits, runtime receives no
partial record or `EvaluatedCommand`; the catalog-owned returned resource
evidence retains only the original bounded raw snapshot plus the exact resolved
plan/proof and no over-budget normalized values or masks. The coordinator treats
the result as `ExecutionFault::ResourceLimit` and may terminalize it under
ADR-0012 only after the ordered current recheck and deterministic overflow
reproduction in Section 9.6. A malformed
proof, foreign writer, unproved omission, or a proof/cap invariant reached after
successful activation is instead integrity and never a resource fault.

After evaluation, only `riffdb-commit` may construct `CommitIntent`. Its checked
constructor combines the unchanged `EvaluatedCommand` with the exact stored
pending identity, original request ID, canonical input hash, five-field
`ExecutablePlanRef`, fixed logical time, `AdmittedActorContext`, separate
`StoredAdmittedProvenanceClaimsV1`, validated partition, and bounded
partition/conflict hashes. The coordinator cannot edit the evaluated mutations,
events, or outcome while adding admission metadata. Neither value contains an
assigned sequence or claims that transaction-current validation has succeeded.

`TXN-030` A `CommitIntent` MUST be fully self-contained and MUST NOT contain references into an API request buffer, parser AST, live storage transaction, or lock-manager internal state.

`TXN-031` Mutation and event ordering inside an intent MUST be canonical so hashing and replay are deterministic.

## 9.6 Commit coordinator

The commit coordinator runs on a dedicated blocking thread or tightly controlled blocking task because embedded storage writes are synchronous. API tasks submit intents through a bounded channel and await a one-shot response.

The commit coordinator is the only component permitted to create idempotency reservations, assign application commit sequences, or drive authoritative write transactions. It opens a write transaction only after deterministic command evaluation has completed. The write transaction is intentionally short: it rechecks identity and dependencies, supplies transaction-current values for the exact compiled validation plan, applies one bounded atomic record set, and commits. Runtime evaluation MUST NOT occur while a storage transaction is held. The storage API MUST NOT expose arbitrary transaction callbacks or a generic public write surface.

For exact historical-plan matching, grammar-v1 index derivation, and pure
transaction-current commit-check evaluation only, `riffdb-commit` directly
consumes the checked `CommandPlan` from `riffdb-contract-ir` and the sole
`riffdb-invariant` evaluator. It mechanically assembles a private owned value
source from frozen normalized input and `tx.time`, transaction-current complete
binding/root observations normalized with the same catalog lineage proof, and
proposed complete post-images. Mutate/create
bindings resolve only to their proposed post-images; read-only bindings and
internal aggregate-root validation reads resolve only to transaction-current
records. Missing, duplicate, out-of-order, or unmatched semantic positions are
`EvaluationError::Integrity`; there is no pre-image/post-image fallback. No
storage transaction, handle, callback, reader, engine object, clock, entropy
source, or asynchronous operation crosses into the evaluator, and neither the
coordinator nor a backend duplicates evaluator logic.

The coordinator always revalidates every influential dependency. If the
`EvaluatedCommand` has no mutations, it does not construct mutable value-source
positions, invoke the commit-check plan, or derive index deltas and
mutation-affected epochs. It still reserves an empty-mutation write plan and,
after validation, assigns a sequence and atomically persists the declared
outcome, provenance, and commit record. If mutations are present, private
validation first proves exact one-per-mutable-binding coverage; only then may the
complete commit-check plan and index derivation consume the proposed post-images.
Missing, duplicate, or extra mutations are integrity failures, never permission
to substitute transaction-current pre-images.

`riffdb-catalog` owns both opaque proof-application operations and their returned
evidence; the coordinator owns when they are invoked. The first call consumes
the complete owned snapshot after the read view closes and returns `Ready` or
resource evidence. After dependency equality, the coordinator passes each
transaction-current raw binding/root observation to that evidence's pure
current-recheck operation before commit-check value assembly or resource-fault
terminalization. The opaque operation proves exact raw equality and repeats the
same deterministic normalization/charge without a catalog lookup or additional
storage I/O. It preserves all present unknown fields. Storage remains IR-blind,
runtime cannot infer ancestry from a numeric version or optional field type, and
WP-100 neither interprets masks nor implements proof application.

The coordinator performs:

1. Open the durable write transaction and start a count-only candidate.
2. Recheck the exact pending admission and idempotency identity.
3. Read and compare every influential transaction-current dependency; on equal
   present records, invoke the catalog-owned opaque current-recheck operation to
   prove exact raw equality and obtain the normalized current record before
   semantic use.
4. For nonzero mutations only, prove exact mutable-binding coverage and
   re-evaluate the complete commit-time invariant plan over current read/root
   values plus proposed post-images; zero mutations skip this step.
5. For nonzero mutations only, derive the exact mutation-affected prefix targets
   from current and proposed index entries; zero mutations retain empty targets
   and index deltas.
6. Read every affected epoch position in the same transaction.
7. Freeze the exact sequence-free write plan and reserve semantic plus conservative encoded capacity.
8. Allocate the next commit sequence only after reservation succeeds.
9. Construct and verify the complete entity/index/epoch/outcome/event/outbox/provenance/commit graph against the retained intent, plan, and assignment.
10. Canonically encode every durable envelope and prove its actual per-class byte charge is within the retained upper bound before staging.
11. Stage the complete record graph and commit the storage transaction using configured durability.
12. Publish the committed result to the waiting caller and subscribers only after durable success.

For the frozen `CommandWriteSetPlanV1`, the durable codec computes every
conservative complete V2 envelope upper-bound charge and its checked aggregate
before sequence assignment. `riffdb-storage-api` exposes only the fields-private
closed result `Fits(EncodedWriteSetUpperBound) |
ExceedsAcceptedAggregateCap`; only the codec can construct it and only
`riffdb-commit` consumes it in production. The second variant can arise only
after every per-record charge and the checked sum succeeded and the final sum
alone exceeded the accepted 16 MiB aggregate cap. The commit candidate maps only
that origin-specific case to `CapacityUnavailable` and the existing public
`StorageUnavailable`: Pending stays byte-identical, no sequence or graph is
written, readiness remains true, and the coordinator is neither stopped nor
fenced. Per-record limit failures, checked-sum overflow, malformed or
noncanonical bytes, unknown type/hash/version, key/envelope mismatch,
under-reservation, retained-plan substitution, and every other codec/integrity
failure remain fatal internal integrity failures. Code MUST exhaustively match
the typed result and MUST NOT classify an error string or a generic
`LimitExceeded` as retryable availability.

For an ADR-0012 arithmetic or resource fault, the coordinator instead opens the
narrow terminalization transaction, rechecks the complete pending admission and
every influential absence/version/epoch, and atomically records
`ExecutionFailed` only while all evidence is equal. It assigns no application
sequence and constructs none of the command atomic record set. A normal
dependency change writes nothing and follows the bounded full-reevaluation
policy; malformed or missing evidence is an integrity incident and leaves the
admission pending.

For a lineage-normalization `ResourceLimit`, "all evidence is equal" has one
mandatory order. The coordinator first compares every canonical dependency. It
then invokes the catalog-owned resource evidence's pure current-recheck operation
with each transaction-current raw binding/root observation. That operation
compares the raw observations exactly, including absence/presence, target,
entity version, writer identity, schema binding, canonical field order, and every
known or unknown field/value, before re-running the same deterministic
normalization and exact charge. The coordinator may terminalize only if that recheck again
produces the same valid over-limit `ResourceLimit`; success, a different fault,
or an impossible proof/cap result is an integrity incident. The recheck performs
no catalog lookup, additional storage read, runtime evaluation, or partial
over-budget retention.

A false transaction-current commit check remains the non-durable
`CandidateValidationRejection::CommitCheckRejected`. A transaction-current
`EvaluationError::Arithmetic` is instead the additive non-durable
`CandidateValidationRejection::CommitCheckArithmeticFault`: the coordinator
abandons and rolls back the application candidate before sequence assignment,
maps it to `ExecutionFailureCode::ArithmeticFault`, and uses the separate
dependency-revalidating terminalization transition above. It discards the one
never-persisted provenance candidate already sourced after successful runtime
evaluation and obtains no second candidate for terminalization. Integrity faults
remain redacted internal incidents, never caller data or predicate rejection.

`riffdb-commit` privately fixes
`MAX_COMMAND_EVALUATION_ATTEMPTS_V1: usize = 3`. An attempt slot is consumed
immediately before catalog materialization of one complete owned snapshot. A
`Ready` result continues into exactly one runtime evaluation in that same slot;
a valid lineage-expansion `ResourceLimit` consumes the slot even though runtime
is not entered. Thus one invocation may perform at most two automatic full
reevaluations. Dependency change,
commit-check rejection, mutation-precondition change, or changed evidence while
terminalizing an execution fault consumes the completed attempt. Before another
attempt, the coordinator proves abort, releases the logical capability, checks
cancellation/deadline, reacquires every canonical conflict key, and materializes
a new complete snapshot. `CommitStatusUnknown` instead fences writes and forces
same-key durable resolution without another attempt.

If attempt three requires reevaluation, no fourth attempt begins. The
coordinator releases non-durable capabilities, discards any unpersisted
provenance candidate, and returns internal `RetryBudgetExhausted`; Pending remains
byte-identical and no sequence, mutation, outcome, event, outbox intent, commit,
execution-failure record, or durable provenance is written. The ceiling is
nonconfigurable, invocation-local, non-durable, absent from IR and hashes, and
resets only for a fresh authenticated and authorized submission.

`TXN-040` The commit queue MUST be bounded and apply backpressure.

`TXN-041` The coordinator MUST NOT assign even an invisible transaction-local
sequence until transaction-current validation, mutation-affected epoch reads,
the exact sequence-free write plan, and semantic plus conservative encoded-
capacity reservation have all succeeded. An assigned sequence becomes visible
only with atomic persistence of its complete verified record graph.

`TXN-042` Sequence allocation, entity writes, idempotency record, command outcome, durable events and outbox intent, commit record, and provenance MUST commit atomically.

`TXN-043` A storage error before durable commit MUST not leave a visible terminal result.

`TXN-044` A process crash after durable commit but before response is an expected recovery case resolved through idempotency.

## 9.7 Cancellation and deadlines

Cancellation is advisory until the durable commit boundary.

- Before lock grant: remove waiter and stop.
- After lock grant but before commit submission: stop evaluation and release lease.
- While waiting in commit queue: cancellation MAY remove the intent if the coordinator has not started it.
- After the coordinator begins a storage transaction: the server MAY complete the commit even if the client disconnects.
- After commit: cancellation never rolls back business state.

For MCP, a cancelled request may receive no response as required by protocol behavior. The agent can resolve the result using the command's idempotency key or outcome resource.

## 9.8 Panic and defect containment

- Runtime and compiler public entry points MUST not panic on user-controlled input.
- A containable panic in command evaluation MUST be caught at the executor/service boundary, recorded as an internal failure with a correlation ID, and release capabilities; it is never converted to ADR-0012 `ExecutionFailed`.
- The commit coordinator process should fail fast on an invariant breach indicating internal corruption rather than continue serving uncertain state.
- Recovery MUST run storage integrity checks and metadata consistency checks before readiness.

---

# 10. Durable storage design

## 10.1 Storage interface

The storage abstraction is semantic, not a generic database portability layer.
`riffdb-storage-api` owns the owned bounded `ReadSnapshot`, canonical dependency
and observation types, `EvaluatedCommand`, `CommitIntent`, semantic durable DTOs,
typed transition/read requests and results, and engine-neutral persistence ports.
It owns only structural constructors; the coordinator retains the private
commit-candidate proof that targets, dependencies, mutations, events, and
outcome match one exact checked historical plan. That proof is distinct from
catalog-owned opaque lineage materialization evidence.

`ReadSnapshot` is an owned value, not a live engine view or trait object. A
synchronous storage call privately opens one consistent read view, copies every
requested complete record and epoch into bounded semantic values, closes all
engine handles, and only then returns. Runtime cannot retain or extend a storage
transaction.

All engine operations are synchronous. The async boundary is the bounded
coordinator queue. Authoritative command writes use ADR-0004's consuming typed
`EmptyBatch`/`NonEmptyBatch` candidate protocol: an empty batch cannot commit;
starting a candidate checks only the command-count ceiling; each candidate must
then progress through admission recheck, influential transaction-current state
read, private plan validation and mutation-affected prefix derivation, same-
transaction affected-epoch reads, exact sequence-free plan and capacity
reservation, sequence assignment, complete graph verification, actual canonical
envelope-bound checks, and atomic record-set staging; only a nonempty batch can
commit. The interface accepts no
closure, callback, raw key/value batch, caller-selected sequence, engine handle,
or async method. Runtime evaluation never occurs while a storage transaction is
open.

Storage capabilities are split into narrow entity/commit readers, catalog,
capability, outbox, projection, administration/audit, integrity, and backup
ports. Only the coordinator receives authoritative progression or audit-append
handles; projection and outbox workers receive only their specialized derived
state transitions. `riffdb-storage-api::proto_codec` is the sole checked bridge
from semantic durable DTOs to `riffdb-proto` messages. No Prost type appears in
semantic trait signatures.

Storage first provides a source-free database-identity probe with the closed
result `Existing(DatabaseId) | NeedsInitialization`; malformed or partial
metadata is an integrity error. Only `NeedsInitialization` permits the server to
generate a checked candidate. A separate initialization transition accepts that
candidate, re-proves true emptiness, and atomically installs it with the complete
initial metadata. If another initializer won after the probe, it returns the
durable winner without comparing or replacing it. Reopen returns `Existing` and
therefore performs no candidate generation. A missing or malformed ID in any
otherwise initialized/nonempty store fails readiness; backup/restore preserve it.
Storage code has no clock or entropy source.

Production invokes the probe and mutating initialization transition only through
the commit-owned `DatabaseInitializationExecutor`. `riffdb-server` may request a
candidate after the executor returns `NeedsInitialization`, but it passes that
checked value back to the executor and never obtains a storage mutation handle.
Memory/redb conformance tests may exercise the storage transition directly; this
does not create a second production authoritative-mutation path.

After the executor has established the durable `DatabaseId`, a production open
enters an exclusive `StructuralEvidenceSession`. This type-state owns and
withholds all backend mutation and operational ports while the open is being
validated. It exposes only bounded, synchronous structural scan operations and
ordered `HistoricalSemanticEvidence` pages. Each page is bound to the durable
database identity and one open session, has a checked continuation, and ends in
one unambiguous exact-end result. A size/finding ceiling, truncated page, dropped
session, repeated or skipped continuation, or structural error is failure, never
successful end-of-history evidence.

The evidence contains only bounded storage-structural facts and canonical stored
bundle/plan bytes needed by the catalog validator. It enumerates every persisted
entity key and index-range prefix as bounded `PersistedKey` evidence. Every
physical V1 or V2 index row instead appears exactly once as the closed
`HistoricalSemanticEvidence::IndexMigrationRow` variant, inseparably binding its
physical key, semantic row, and exact canonical registered envelope bytes from
the storage-owned codec. The variant replaces, and never accompanies, an index
`PersistedKey` item. Storage checks framing, bounds, canonical bytes,
physical-key/record-key equality, record reciprocity, ordering, and session
origin but does not interpret a `KeySchema` or partition expression. The catalog
must consume exact end, validate every key against the selected historical
schema, and derive/check every index row's historical partition before it may
construct readiness or migration context; an unknown owner/schema, incomplete
component, schema mismatch, omitted/duplicate row, or truncated stream fails
closed. This changes no durable key encoding and introduces no storage-to-IR
dependency.

`IndexMigrationRow` remains in the historical stream's `0x04` domain with exact
order key
`0x04 || u32_be(lineage_length) || lineage || u64_be(contract_version) ||
bundle_hash[32] || 0x02 || u32_be(index_id) ||
u32_be(index_entry_key_length) || index_entry_key`. Its checked general-page
semantic charge is exactly
`1 + 4 + physical_key_length + 4 + canonical_envelope_length`, using checked
addition, and the same row simultaneously counts against the independent
500-row/4-MiB migration-row bound. Capability-partition evidence remains tag
`0x05`; no `0x04` item may follow it.

The evidence carries no decoded IR and grants no operational mutation authority.
When the structural scan and every evidence cursor reach exact end, consuming
the session and both backend exact-end authorities returns
`StructuralOpenOutcome::Clean(StructurallyOpened)` only if the backend observed
no V1 index row, or
`StructuralOpenOutcome::MigrationRequired(StartupIndexMigrationPort)` if it did.
The catalog independently returns
`CatalogHistoryOutcome::Ready(ValidatedCatalogHistory)` only for a V2-only
history, or
`CatalogHistoryOutcome::MigrationRequired(CatalogIndexMigrationContext)` after
observing V1. The server may join only `Clean` with `Ready`, or consume the two
matching `MigrationRequired` values into the catalog-owned migration driver. A
crossed pair is integrity failure. The storage-side result is a named concrete
port that privately implements ADR-0042's identity-only
`StartupIndexMigrationPort` contract and is produced only by the consumed
structural session; its constructor is private and it exposes no public scan, bundle-read,
apply, exact-end, or finish operation. Migration values have no readiness
conversion, current-recheck operation, operational accessor, reusable
authority, callback, or parallel handle. On any failure, the entire open is
dropped and no dormant or operational port is released.

The server drives the evidence pages into the catalog-owned historical validator.
Only server composition may combine matching `StructurallyOpened` ports and
opaque same-session `ValidatedCatalogHistory` with the required readable
digest inventories and lifecycle checks to create operational wiring. The proof
does not pass back through storage. Memory/redb receive no IR and may depend on
catalog only through ADR-0042's sealed startup migration driver; that edge
grants no historical validator, readiness proof, or catalog-persistence access.
The memory engine implements the same initialization-first,
exclusive-session, exact-end, session-binding, migration, and dormant-port
type-state for conformance tests.

The startup-only `StartupIndexMigrationPort` contract is identity-only and
remains outside every operational storage trait. The named memory/redb concrete
ports are linear, privately constructed, and bound to their original
`DatabaseId` and `OpenSessionId`. Catalog owns the consuming driver and every
instruction, batch, pending, and completion value. Storage API owns no catalog
authority/tracker/advance/completion, instruction/page/batch chain,
`StartupIndexMigrationEnd`, `StartupIndexMigrationRead`, or public page-read
operation.

Every cross-crate backend scan, one-bundle read, batch apply, and finish call
consumes a catalog-owned, fields-private, move-only request/proof branded by the
exact concrete backend type `B`; its backend response factory may be called only
by consuming that exact request and returns the next branded state. Thus a local
fake backend cannot capture `Batch<Fake>` and replay or convert it into
`Batch<Redb>`. A named public migration-only backend trait is permitted only
under this branded consuming-factory rule. Catalog-owned branded request and
response types may be public only as required for concrete trait
implementation; their fields and free factories remain unavailable, and the
server is forbidden from importing or receiving them. Concrete backend page,
point-read, apply, exact-end, and finish helper states remain module-private.

Inside that sealed driver, the concrete backend rescans V1/V2 physical index
rows in strict physical-key order through short read transactions. Each
session-bound backend-private page enforces two independent ledgers before
release: at most 500 rows and 4 MiB for exact observed row evidence, and at most
500 rows and 4 MiB for conservative complete instruction/write charges. It
stops before the first row that would exceed either ledger, makes that
unconsumed row the strictly advancing continuation, and cannot emit an empty
nonterminal page when a valid row remains. These bounds do not lower the
separately accepted 15 MiB immutable bundle or general historical-evidence
limits.

WP-065 MUST prove from accepted key/value/binding/payload/envelope/framing maxima
that one maximum valid row and its conservative complete V2 replacement fit in
4 MiB. A semantic row is never split. Failure of that proof is an authoritative
bound conflict requiring human review, not permission to raise a limit or loop
without progress.

For each current row, a paired same-session point read supplies at most its one
exact retained historical bundle after the storage transaction closes. The
catalog derivation accepts that exact owned canonical bundle and codec-checked
row; it never accepts a caller-supplied partition or V2 post-image. Catalog
selects the historical owner/schema, decodes the complete embedded entity key,
binds the root-key positional prefix, and uses only the pure invariant evaluator
to derive the exact historical partition. It consumes the row into exactly one
fields-private move-only `V1Rewrite` instruction binding the complete expected
V1 evidence and derived V2 post-image, or one `V2Confirm` instruction binding
the complete expected V2 evidence after equality proof. The concrete backend
consumes a complete page and the catalog-owned opaque instruction batch
together, constructs replacement envelopes only through the durable codec,
compares every current value, and atomically applies all V1-to-V2 replacements
or none. An exact already-written V2 is idempotent replay for `V1Rewrite`;
`V2Confirm` writes nothing. Any absence or byte/semantic mismatch aborts as
corruption.

Only the catalog-owned driver may join its exact same-session, fields-private
completion to the backend-private exact end; that operation yields only dormant
unopened backend state. The server sees neither value. It discards every pre-
migration proof, begins a fresh
session with a fresh `OpenSessionId`, and repeats complete structural and catalog
validation. Only a fresh V2-only `Clean` plus `Ready` pair can reach readiness.
An immediate second migration requirement after an in-process completion is
integrity failure. Crash or cancellation drops the port; restart begins the
complete process again. There is no migration marker, added metadata category,
online migration, persisted continuation, or readiness concurrent with
migration.

The concrete `redb` adapter may contain private compaction, backup, integrity,
and statistics APIs. The approved baseline is exactly `redb` 4.1.0 with default
features disabled and no optional features, directly owned only by
`riffdb-storage-redb`; any dependency-graph or feature change requires renewed
human review. Approval of the dependency does not replace WP-070 semantic
conformance and process crash/reopen evidence.

The v1 semantic storage hard ceilings are 4,096 binding observations, read
dependencies, validation targets, mutations, index deltas, event intents, or
outbox intents per command; 1 MiB per canonical entity/event/outcome value;
16 MiB per owned snapshot; 15 MiB per pre-commit intent or commit-record semantic
payload; 64 commands and 16 MiB aggregate staged write set per write transaction;
500 rows and 4 MiB per generic scan page; independently, 500 complete index rows
and 4 MiB for each startup migration evidence page and each conservative
instruction/write batch; 500 records and 16 MiB encoded content
per internal ordered commit-scan page; 4 KiB per
entity/index/partition/conflict key or index prefix; 256 integrity findings plus
a `truncated` flag; 15 MiB per catalog bundle; 4,096 bundles and 64 MiB exact
canonical bundle bytes per active lineage; 2 MiB per process-local catalog
lineage-materialization proof; an independently derived maximum 512-byte
null-fill mask per record position and 2 MiB structural maximum across 4,096 binding/root
positions; 16 targets and 64 KiB per service-audit record; and ADR-0006's
absolute 16 MiB durable payload/envelope ceiling. Every input, runtime result,
component count, semantic value, and component byte bound that is knowable before
transaction open uses checked arithmetic and is rejected before unbounded
allocation or opening that transaction. The sole deferred aggregate calculation
is the exact staged-write capacity reservation derived from transaction-current
old values, affected epochs, and the sequence-free plan; it occurs inside the
transaction after current reads and before sequence assignment. WP-065 defines
and proves conservative canonical `StoredEnvelope` upper bounds per record class,
and WP-070 recomputes actual canonical envelope bytes and rejects any excess
before staging. Configuration may lower, but never raise, a hard ceiling.

An authoritative index-scan page returns its rows and the exact closed
`IndexEpochPosition::{BeforeFirst, Value(IndexEpoch)}` observed in one storage
read view. An initial ordered commit scan similarly returns the page and the
authoritative `FrontierPosition` upper bound from one read view; every
continuation is bounded by that frozen inclusive upper position and ignores
later commits. No storage transaction or snapshot handle crosses a page,
service, policy, or transport boundary. Empty index and commit states remain
explicit rather than being mapped to a fabricated nonzero value.

The active-lineage count, canonical-byte sum, proof charge, and mask charge use
the exact Section 8.5 definitions. They add no storage-engine allocation or
envelope charge to canonical bytes; retained normalized snapshot semantic bytes
plus all retained nonempty mask bitset payload bytes share, rather than add to,
the existing 16 MiB snapshot budget; aligned vectors make mask position implicit
without a metadata charge. They do not relax the 1 MiB record, 16 MiB
snapshot, 15 MiB bundle/intent, 16 MiB evidence/write-set, or generic backup
limits. Compile-time arithmetic assertions and exact-boundary tests MUST fail if
a constituent IR bound could exceed its approved proof or mask ceiling.

For a mutating grammar/IR-v1 plan, checked plan construction also proves the
conservative cross-product ceilings for index-entry deltas, mutation-affected
prefix targets, their combined validation positions, and affected epoch-state
bytes described in Section 9.4. This check occurs before plan hashing and bundle
activation. The short transaction still enforces every exact dynamic bound
incrementally before retaining item 4,097 or more than 16 MiB.

`StorageErrorKind` is closed: `Unavailable`, `CommitStatusUnknown`,
`CorruptData`, `IncompatibleFormat`, `LimitExceeded`, `InvariantViolation`, and
`SequenceExhausted`. A backend-proven abort maps to unavailable and never claims
terminal success. Unknown commit status fences further writes until
reopen/integrity recovery and command resolution uses idempotency. Corruption,
incompatibility, impossible transitions, and exhaustion fail readiness with an
opaque incident. Missing records, replay, mismatch, pending admission,
dependency change, catalog conflict, already-revoked capability, projection gap,
and scan end are typed semantic results, never parsed error strings.

If any mutation-affected index epoch is already `u64::MAX`, its attempted
advance returns internal `SequenceExhausted` and aborts the complete transaction
before application-sequence assignment. Pending remains byte-identical and no
command graph is durable. The current executor call returns the opaque internal
`InternalDefect`, the coordinator transitions to stopped/unready, and queued or
future work returns `CoordinatorStopped`. This proven abort is neither
`StorageUnavailable` nor `OutcomeUnknown`, and it never transitions the actor to
the uncertain-write `CoordinatorFenced` state.

## 10.2 Redb table layout

| Table | Key | Value |
|---|---|---|
| `meta` | UTF-8 system key | Versioned metadata value |
| `contract_bundles` | lineage byte length as `u32` big endian + exact lineage UTF-8 + contract version as `u64` big endian | Contract bundle envelope |
| `catalog_active` | exact byte `0x01` | Active contract version and hash |
| `entities` | exact canonical `EntityKey` bytes | Entity record envelope |
| `secondary_indexes` | exact canonical `IndexEntryKey` bytes | Index entry envelope including covered values |
| `index_epochs` | exact canonical index-range-prefix bytes | Monotonic validation epoch envelope |
| `idempotency` | canonical identity from Section 7.6 | One full `StoredOutcomeV1` envelope serving as both committed terminal state and persisted outcome, or one closed ADR-0012 `StoredExecutionFailedV1` envelope; never a pointer or tombstone |
| `idempotency_pending` | canonical idempotency identity | Pending reservation, fixed transaction context, complete `ExecutablePlanRef`, admitted actor, and stored approved provenance claims |
| `commits` | commit sequence, big endian | Commit record envelope |
| `provenance` | exact 16-byte provenance ID | Immutable approved provenance record envelope linked from one commit |
| `events` | exact 12-byte event ID | Authoritative durable event envelope |
| `outbox` | exact 12-byte event ID | Outbox-intent envelope containing the same event |
| `outbox_status` | exact 12-byte event ID | Delivery state and attempts |
| `projection_state` | `0x47 0x01` + projection identity + generation + framed complete group components | Versioned aggregate state and last-changed sequence |
| `projection_frontier` | `0x46 0x01` + projection identity | Versioned control record with highest generation, published/candidate frontiers, lifecycle, apply mode, and closed failure |
| `projection_applied` | `0x41 0x01` + projection identity + generation + commit sequence | Versioned marker containing the exact `ProjectionApplyHash` |
| `capabilities` | `0x01` + stable `CapabilityId` | Versioned capability grant and lifecycle record with repeated typed digest reference |
| `capability_tokens` | `0x01` + digest scheme + digest-key ID + digest | Versioned lookup containing exactly one `CapabilityId` |
| `audit` | `0x01` + administration sequence | Versioned registered catalog/capability-administration or service-audit record in one ordered sequence space with disjoint closed payload registries |

The only `meta` keys are the exact UTF-8 strings `format_version`,
`database_id`, `next_application_sequence`, `next_administration_sequence`, and
`capability_bootstrap/v1`. The active-contract metadata category is represented
once by `catalog_active/0x01`; it is not duplicated in `meta`. Physical keys use
the complete canonical key bytes named above and MUST NOT prepend a redundant
entity type, index ID, or other owner field already present in that canonical
encoding. The POC persists only the frozen primary tables in this section;
backend-private accelerators are rebuilt from authoritative records and are not
unversioned durable state.

ADR-0050's checksummed maintenance receipts live only beneath the configured
external `backup_root/.maintenance` subtree. They are not database metadata,
not a redb table/key, not a `StoredEnvelope`, not copied into a backup
artifact, and not counted as a seventh category. They are a separately
versioned external operational format used only to audit and recover the one
offline operation that closes or replaces this database.

`STO-010` Ordered numeric keys MUST use big-endian encoding so lexicographic scans preserve numeric order.

`STO-011` Table names and key prefixes are storage-format API and require migration planning after POC format freeze.

`STO-012` The POC durable operational metadata MUST contain exactly these six
categories: storage format version; the permanent `DatabaseId`; application
commit allocator state; administration audit allocator state; the active
contract pointer and its catalog-consistency data; and the singleton
`capability_bootstrap/v1` marker. Application and administration allocator
metadata starts at 1; zero is unassigned and every advance uses checked
arithmetic. Committing the maximum representable sequence atomically leaves the
corresponding allocator in an explicit exhausted semantic state; it never wraps
or advertises another numeric value.

The POC defines no durable node identity, clean-shutdown marker, or persisted
last-successful-integrity-check value. Every production startup MUST run the
complete accepted read-only authoritative integrity and metadata-consistency
validation before readiness, regardless of whether the prior process terminated
gracefully. Graceful shutdown remains required process-lifecycle behavior but
MUST NOT write a clean-shutdown marker or substitute authoritative metadata.

## 10.3 Record envelope

```protobuf
message StoredEnvelope {
  uint32 storage_format_version = 1;
  string record_type = 2;
  bytes payload = 3;
  fixed32 payload_crc32c = 4;
  bytes schema_hash = 5;
}
```

The payload is a typed Protobuf message. The outer checksum provides early corruption detection independent of the embedded engine's own integrity mechanisms.

`STO-020` Durable formats MUST be explicitly versioned.

`STO-021` Unknown future record types MUST cause a controlled startup error, not silent deletion or reinterpretation.

`STO-022` POC storage migrations MAY be offline but MUST be restartable and idempotent.

The `riffdb.storage.v1` readable compatibility registry contains exactly the
accepted 26 top-level `StoredEnvelope` payload tuples below, with their existing
descriptor closures and schema hashes byte-identical, plus one additive tuple
for `StoredIndexEntryV2`:

1. `StoredStorageFormatVersionV1`
2. `StoredDatabaseIdentityV1`
3. `StoredApplicationSequenceAllocatorV1`
4. `StoredAdministrationSequenceAllocatorV1`
5. `StoredContractBundleV1`
6. `ActiveCatalogPointerV1`
7. `StoredCatalogAdministrationV1`
8. `StoredEntityRecordV1`
9. `StoredIndexEntryV1`
10. `StoredIndexEpochV1`
11. `StoredPendingAdmissionV1`
12. `StoredExecutionFailedV1`
13. `StoredOutcomeV1`
14. `StoredDurableEventV1`
15. `StoredOutboxIntentV1`
16. `StoredProvenanceRecordV1`
17. `StoredCommitRecordV1`
18. `CapabilityRecordV1`
19. `CapabilityTokenLookupV1`
20. `CapabilityBootstrapMarkerV1`
21. `CapabilityAdministrationAuditV1`
22. `ServiceAuditRecordV1`
23. `StoredOutboxStatusV1`
24. `StoredProjectionStateV1`
25. `StoredProjectionApplyV1`
26. `StoredProjectionControlV1`
27. `StoredIndexEntryV2`

`StoredIndexEntryV2` is defined alone in
`proto/riffdb/storage/v1/index_v2.proto`, which imports the unchanged
`riffdb/storage/v1/application.proto`, with exact fields
`bytes index_entry_key = 1`,
`DurableKeySchemaBindingV1 schema_binding = 2`,
`bytes canonical_covered_values = 3`, and `bytes partition_key = 4`.
`application.proto` and the other eight pre-V2 durable sources remain
byte-identical, so the generated durable-source inventory is exactly ten. Only
the V2 FQN receives a new descriptor closure and schema
hash. The outer `StoredEnvelope`, storage-format version, physical
`secondary_indexes` table, and complete physical `IndexEntryKey` are unchanged.

The writable role registry contains exactly 26 entries: the unchanged 25
non-index roles plus V2 as the sole current index-entry role. V1 is decode-only
for migration and has no current encoder or normal-write lookup. Tests MUST
freeze exact ordered FQNs, hashes, and readable/writable membership; a count
alone is not sufficient. A pre-V2 binary continues to reject the unknown V2
tuple, so rollback to it after migration is unsupported. Backup and restore
preserve exact envelope bytes and reopening restored V1 or mixed state runs the
same exclusive restartable migration and fresh-validation sequence.

The capability and service-audit wire names in that list are fixed even where
the Rust semantic DTO uses a `Stored*` name. Closed helper messages, including
read-dependency collections and capability grants/permission sets, remain nested
and are not separately registered envelopes. No other speculative reserved
field, record type, key codec, ADR-0019 deferred metadata, or later-feature
placeholder is part of the v1 registry. Any further addition, removal, rename,
field-number change, enum/oneof tag change, or top-level/nested reclassification
is a durable-format change requiring compatibility and recovery review.

## 10.4 Commit record

The semantic commit record contains the assigned nonzero sequence, original
admission request ID, complete five-field `ExecutablePlanRef`, canonical input
hash, admitted actor/logical time/partition and redacted conflict identities,
canonical read dependencies, complete mutation post-images, ordered durable
event identities/payloads, original declared outcome, and links to the immutable
provenance and outbox intent created in the same atomic record set. Replay is
response metadata and never creates or modifies a commit record.

Every `StoredOutcomeV1` must reciprocate with exactly one
`StoredCommitRecordV1` at its nonzero commit sequence, and every committed
command record must have exactly one corresponding outcome row. Startup checks
the outcome table key against the payload identity and requires the outcome and
commit to agree exactly on their shared admission request, plan, canonical input
hash, actor, logical time, partition/conflict hashes, declared outcome,
provenance link, sequence, and durability mode. The linked provenance record
must carry the same idempotency identity. A missing, duplicate, mismatched, or
still-pending reciprocal record is corruption and is rejected without repair.

Each durable event carries an `EventHash` under the existing unkeyed
`riffdb.event/v1` domain. The exact domain-frame payload is:

```text
EventId canonical bytes (CommitSequence:u64_be || event_ordinal:u32_be)
|| EventTypeId:u32_be
|| payload_length:u32_be
|| canonical Value::Record payload bytes
```

The event ID is therefore exactly 12 bytes. `payload_length` is the exact byte
length of the complete canonical record value, including its canonical value
format byte and record tag. Hashing only the business payload, omitting identity
or type, using a different length width/order, or hashing Protobuf bytes is
noncanonical. Construction, durable decode, startup integrity, and reciprocal
commit/event/outbox validation MUST recompute and compare this exact hash.

Each durable event is also stored as its own `StoredDurableEventV1`
`StoredEnvelope` in the authoritative `events` table under its exact 12-byte
`EventId`. The standalone event row, the event nested in the corresponding
`StoredOutboxIntentV1`, and the event nested in `StoredCommitRecordV1` commit
atomically and MUST be exactly equal. A missing, duplicate, orphaned, unequal,
or hash-invalid copy is corruption and MUST be rejected without repair.

The pre-commit `CommitIntent` carries the coordinator-generated checked
`ProvenanceId`; storage never generates or substitutes it. A collision is an
atomic invariant failure with no sequence, overwrite, or partial record. A
proven-aborted retry may use a different invisible candidate, while unknown
commit status must be resolved through idempotency before requesting another.

Sensitive command input values MUST NOT be recorded by default; only the
canonical hash and policy-approved admitted provenance fields are retained.
WP-065 assigns the exact versioned Protobuf message and field numbers after the
storage semantic DTO is frozen. This section intentionally does not pre-empt
that proto-owner review.

## 10.5 Durability modes

| Mode | Behavior | Use |
|---|---|---|
| `sync` | Commit is acknowledged only after the embedded engine reports durable synchronization. | Required P1 `riffdbd` correctness mode |
| `group` | Coordinator batches compatible intents and performs one durable flush for the batch. | Semantic interface and benchmark experiments only in the POC |
| `memory` | No durability guarantee. | Unit and model tests only; server refuses non-test startup |

The returned outcome MUST identify the durability mode used for its commit.
Every production commit-coordinator constructor MUST receive an explicit closed
process-local durability value whose variants are `sync` or `group`; it MUST have no
implicit or trait-provided default and MUST NOT infer a mode from the backend.
`memory` may be supplied only through test-only coordinator construction and is
not a production constructor value.

The POC production server exposes only `sync` durability. Production `group`
mode remains disabled unless the WP-100 scheduling, fairness, latency, and crash
evidence receives explicit human review; defining the semantic mode and measuring
it does not enable it. Its possible MVP default remains a post-POC decision.
The P1 `riffdbd` component graph explicitly passes the code-level `sync` value to
coordinator construction; it does not obtain `sync` from a constructor or
backend fallback and exposes no POC operator durability selector.

## 10.6 Recovery

Startup recovery first performs a staged, read-only authoritative integrity
phase. WP-070 completes every storage-structural check and emits exact-end
historical evidence while its operational ports remain dormant; WP-050 performs
the IR-aware historical catalog checks; WP-130 is the first production owner that
may join the two matching same-session results and evaluate readiness:

1. Open the database and run engine integrity checks appropriate to configuration.
2. Validate the storage format and permanent durable `DatabaseId`. ADR-0019
   defers a distinct durable node identity beyond the POC.
3. Structurally enumerate every historical bundle/plan reference and the active
   catalog relationship through exact end. The catalog then resolves and
   semantically validates every referenced IR version and plan hash from that
   same session. It also reconstructs the exact active-to-genesis parent/hash
   chain, revalidates each adjacent successor forward, enforces the 4,096-bundle,
   64 MiB exact-canonical-byte, and 2 MiB process-local proof ceilings, and
   derives field-introduction ordinals without trusting stored compatibility
   reports alone. An absent pointer is a valid initialization state only while the
   contract-bundle table and every application-authoritative table are empty;
   capability, bootstrap, and administration-audit records may already exist.
   Any bundle, application commit/state, active-pointer mismatch, unknown plan,
   broken/gapped/cyclic/substituted lineage, aggregate-limit excess, or semantic
   validation failure outside that state keeps readiness false and is exposed
   only as a redacted internal defect. Broken or over-limit active-lineage
   evidence is specifically `InvalidHistoricalEvidence`; other failures retain
   their existing typed catalog classifications.
4. Verify that application commits are contiguous and application-sequence
   metadata is exactly the checked successor of the last commit, or the first
   sequence when empty, with an explicit exhausted state when no successor
   exists. Verify the same invariant independently for the contiguous shared
   administration stream and administration-sequence metadata. Any gap,
   mismatch, invalid before-first state, or exhaustion inconsistency fails
   readiness; the POC performs no online repair. A matching explicit exhausted
   state is structurally valid rather than corruption, but authoritative
   readiness remains false because no further sequence can be assigned.
5. Verify every pending and terminal idempotency identity uses a supported digest
   scheme and a readable `DigestKeyId`. Verify committed-outcome pointers
   reference matching commit records and ADR-0012 `ExecutionFailed` records have
   no sequence, outcome, event, outbox, or command-provenance cross-link.
6. Verify pending reservations are structurally valid and can be safely resumed
   or resolved by retry without allocating a sequence during recovery.
7. Verify reciprocal identity and payload linkage among every commit-record
   event reference, durable event, and authoritative outbox intent. Reject an
   orphan, duplicate, missing counterpart, mismatched `EventId`, or unequal
   canonical event identity/payload. Recompute the exact ADR-0011 `EventHash`
   for every event and require equal hash copies across the reciprocal graph.
8. Verify capability-record/token-lookup one-to-one integrity; every bootstrap
   marker, bootstrap `started` record, capability-administration record, and
   create/revoke sequence and timestamp cross-link; exact target capability and
   record kind; and readable configured digest support for every active
   unexpired capability. A missing, duplicate, mismatched, wrong-kind, wrong-
   target, or unsupported cross-link fails readiness.
9. Require the `RetainedMetadataV1` carried inside `StructurallyOpened` to match
   its database identity and the catalog proof's absent or exact active pointer.
   Use that same-snapshot value for bootstrap-marker lifecycle selection and
   require both independent allocator states to be `Next(_)` before readiness.
10. Rebuild authoritative in-memory indexes, lock metrics, and subscriptions only
    after the matching `StructurallyOpened`/`ValidatedCatalogHistory` pair and
    retained-metadata checks are accepted.

If the first complete scan observes any V1 index row, steps 1 through 10 do not
release readiness or operational ports. Storage and catalog must both return
their same-session `MigrationRequired` types, the server must drive the exact
bounded linear migration from Section 10.1, and completion yields only dormant
unopened backend state. Recovery then restarts this entire list under a fresh
`OpenSessionId`; only an all-V2 scan whose storage and catalog outcomes are
respectively `Clean` and `Ready` may continue. Mixed V1/V2 state is a normal
restartable migration input only before that fresh validation. A crossed
outcome, mismatched row/instruction, second migration requirement immediately
after in-process completion, or any post-migration V1 row is corruption. No
startup transaction remains open during catalog evaluation, and no rollback to
a binary that cannot read V2 is supported.

`StructurallyOpened` alone never means readiness. Its completed same-session
handoff includes the exact structurally checked `RetainedMetadataV1`, but that
value is not a catalog proof or operational authority. WP-130 authoritative
readiness becomes true only after matching same-session `ValidatedCatalogHistory`,
retained identity and active-pointer agreement, digest-provider inventories,
and all remaining checks succeed, both allocators can progress, and an active
contract is valid. A truly empty database may pass structural and
catalog integrity while remaining in `Initializing`, not ready. In that mode the
server exposes only the restricted pre-bootstrap health view defined in Section
16.3 plus the exact loopback bootstrap operation; after
bootstrap it additionally permits authenticated contract deployment through the
ordinary service/coordinator path. No command or general read path is enabled
until deployment atomically establishes the active contract.

Derived-component recovery is separate and cannot rewrite authoritative source
state:

1. WP-070 reports bounded structural findings for projection rows/control/
   markers and outbox delivery status without applying worker policy or blocking
   otherwise healthy authoritative writes solely for a derived fault. Its
   worker-only outbox recovery scan freezes an inclusive upper `EventId`, pages
   reciprocal event/intent/status observations in strict order through explicit
   exact end under the generic 500-item/4-MiB limits, includes every undelivered
   effective state, and never treats the pending accelerator as completeness
   evidence. Memory/redb rebuild that non-durable accelerator on every open.
2. WP-160 interprets an outbox intent with no status row as never-attempted
   `Pending` and idempotently normalizes a prior `Delivering` status to the
   policy's retryable pending state before the dispatcher becomes ready. A
   compare-and-transition race is reread; it is never overwritten blindly.
3. WP-170 verifies each retained projection generation's reciprocal contiguous
   markers, apply hashes, lifecycle, pointers, and state keys. A component fault
   is `Degraded`; recovery retires/quarantines the failed candidate as specified
   and rebuilds only into a fresh generation. It never repairs commits/entities
   from projection state or lowers a published frontier. Storage's internal
   degraded result carries identity, optional retained generation, and frontier
   from one snapshot; invalid carries exact identity, failed generation, and
   frontier. The service validates that fence before stripping the internal
   generation from the unchanged public result and performs no second status
   read.
4. WP-185 reports projection/outbox recovery failures as component degradation
   while preserving the distinct authoritative readiness signal. A shared engine
   corruption that prevents trustworthy table isolation remains an authoritative
   storage failure, not a derived exception. With no configured production
   outbox destination, pending events remain pending and health reports bounded
   degradation; no discarding delivery or dead-letter transition is invented.

Offline maintenance recovery is separately governed by ADR-0050. Every
incomplete external receipt is checksum-validated and reconciled against exact
artifact evidence before ordinary readiness. Backup reopens the unchanged
database through this complete startup path. Restore validates a private staged
database, performs fresh staged authentication/authorization, publishes only
the exact verified artifact, and repeats the complete startup path after
publication. No protected polling or new request is served while redb is
offline, and a terminal receipt must be durable before readiness.

`REC-001` Recovery MUST be idempotent.

`REC-002` Repeated restart after a crash MUST not create new commits, events, or outcome records.

`REC-003` Projection state MAY be discarded and rebuilt into a new generation
from commits; source entities and commit records MUST not depend on projection
state for correctness. Rebuild MUST NOT lower the published frontier of one exact
`ProjectionIdentity` or expose candidate rows.

## 10.7 Storage benchmark gate

At POC exit, the team MUST implement the same semantic storage tests and benchmark suite for Fjall. The MVP engine decision uses:

- Synchronous and group-commit latency.
- Concurrent read behavior during writes.
- Range-scan and projection catch-up performance.
- Database size and write amplification.
- Recovery behavior and tooling.
- API stability and migration risk.
- Pure-Rust and transitive dependency requirements.

No application or protocol API may expose `redb`-specific types.

---

# 11. Public gRPC API

## 11.1 Principles

- gRPC is the primary programmatic API for applications and internal tools.
- Business outcomes are data, not gRPC error codes.
- gRPC status codes are reserved for malformed requests, authentication and authorization failure, unavailable server, deadline, incompatible protocol, and internal errors.
- Every mutating command call requires a request ID, contract-declared idempotency key, authenticated transport credential, and expected contract version or explicit active-version behavior. The caller never supplies a trusted actor context or provenance decision.
- The outer `request_id` identifies a transport submission. The caller's command `idempotency_key` is a separate typed command input and is the uncertainty-recovery identity component.

The API-neutral service surface is six object-safe operation-specific traits:
`ContractApplication`, `CommandApplication`, `QueryApplication`,
`CommitApplication`, `AdministrationApplication`, and `DiscoveryApplication`.
Together they own the closed ADR-0007 operation inventory: validate/explain/
deploy/get active/get version; execute/resolve outcome; entity/index/projection
query and projection status; commit get/scan/subscribe and provenance trace;
health/statistics/capability create/revoke/outbox status; and policy-filtered
command/resource discovery. There is no catch-all enum/payload method, generic
read, generic mutation, SQL, or raw administration call. An aggregate service
handle only groups those six traits.

ADR-0040 exposes exactly six already-existing API-neutral operations through the
public protocol: `GetContractVersion`, `DiscoverCommandTools`,
`DiscoverResources`, `GetProjectionStatus`, `TraceProvenance`, and
`ListPendingOutboxDeliveries`. This adds no semantic operation, service trait,
generic resource read, or alternate authorization path. The two discovery
operations support full pages, compact-observation pages, and a conditional
`CatalogUnchanged` result under a semantic fence containing only active/no-active
catalog identity plus the ordered two-schema operation-catalog identity.
`DiscoverResources` additionally requires the closed kind `All | Concrete |
Template`; the service filters that kind before whole-item pagination and binds
it into cursor state. No adapter filters a mixed service page.

The public presentation fence adds one required 16-byte process generation.
`riffdb-server` samples it independently exactly once per production graph-
activation attempt with one `getrandom::fill`, with no retry, clock, fallback,
derivation, reuse, or global state; failure prevents activation/readiness. It is
public opaque comparison data, not a UUID, cursor, durable identity, authority,
runtime input, or uniqueness proof. The gRPC and hosted-HTTP adapters alone
compare/join the same lifecycle-owned value. A generation mismatch discards the
prior semantic fence and forces an ordinary freshly authorized first page; the
API-neutral service never receives process entropy.

Every operation normally accepts a checked `RequestContext` and obtains a
current policy decision. The sole read-only context exception is
`AdministrationApplication::health` during the pre-marker lifecycle described
in Section 16.3. That method accepts a closed service-owned `HealthContext` whose
authenticated variant contains the ordinary `RequestContext` and whose
pre-bootstrap variant contains only a privately constructible,
nonserializable `PreBootstrapHealthContext`. No other operation accepts that
context, and it is not a credential, principal, policy decision, authorization
fact, or storage capability. This exception adds no RPC or alternate health
operation.

Adapters own transport decode, credential extraction, structural conversion
into checked pre-schema submitted DTOs, deadline/cancellation propagation, and
total result mapping. They do not select schemas or construct canonical decimal,
money, enum, or named-record values. This boundary applies both to command input
and to the leading components of index and projection queries. After selecting
the exact active or historical plan, the service resolves submitted names/IDs
and recursively materializes command input and query components against the
selected declared types. Only those canonical query components enter policy
facts, cursor bindings, lower requests, projection waits, or response
validation. The service owns semantic validation, current authorization,
approved provenance, audit orchestration, obligations/redaction, and safe
release. Typed WP-100 executors
own all work after an authorized command/control-plane preparation is accepted.
Consumer-owned bounded ports expose authoritative reads and later projection,
outbox, and operational sources without giving the service a general storage
engine.

Pagination uses server-side opaque cursors. A token is exactly 16 random bytes;
the registry binds it to principal/capability context, operation, target, policy
fingerprint, sanitized request fingerprint, lower continuation, and 300-second
expiry. Limits are 4,096 live server-wide and 64 per principal. Every page
reloads current policy and invalidates on policy or ADR-0017 projection fence
change. Cursors are neither durable nor authorization proofs and cannot be
decoded or forged by adapters. Waits and streams hold no storage transaction,
materialized snapshot, conflict lease, synchronous mutex guard, or runtime frame
across `.await` and recheck current policy before each visible result.

`riffdb-service` owns injected `CursorTokenGenerator` and
`CursorMonotonicClock` consumer ports. WP-130 supplies OS entropy and a private
`std::time::Instant`-origin provider; tests use explicit tokens/ticks and never
sleep. Cursor time is process-relative, non-durable, and disjoint from the four
wall-clock domains. Regression or arithmetic overflow fails closed, and restart
invalidates every cursor.

Service hard bounds are 1 MiB per structurally decoded unary request, 4 MiB per
service unary response, 500 page items (default 50 when omitted), 30 seconds per
projection/read wait, 128 commit subscribers, 256 buffered items per subscriber,
500 commits per catch-up batch, and 900 seconds per subscription. Command
preparation observes idempotency state at most three times and cursor generation
attempts at most three times. Source repository, source commit, reason, and
approval-reference claims are respectively bounded to 512, 128, 1,024, and 256
bytes. Configuration may lower but not raise these POC hard bounds.

Full discovery has the stricter conservative service-response ceiling
2,621,440 bytes. A full item is indivisible and pages may contain fewer than the
requested limit to fit. Compact items are at most 4,096 bytes each; 500 such
items plus page/cursor/fence overhead fit beneath 4,194,304 bytes. Every fence is
required, every present cursor is exactly 16 opaque bytes, an empty page has no
cursor, and continuation state binds operation, representation, resource kind
where applicable, normalized limit, request fingerprint, policy, and catalog
fence. A conditional unchanged result is allowed only for an initial compact
request with a present equal prior fence and no cursor, after fresh
authorization and normal audit completion; it carries no item, cursor, or schema
body and grants no read/invocation authority.

## 11.2 Services

```protobuf
service ContractService {
  rpc ValidateContract(ValidateContractRequest) returns (ValidateContractResponse);
  rpc ExplainCommand(ExplainCommandRequest) returns (ExplainCommandResponse);
  rpc DeployContract(DeployContractRequest) returns (DeployContractResponse);
  rpc GetActiveContract(GetActiveContractRequest) returns (GetActiveContractResponse);
  rpc GetContractVersion(GetContractVersionRequest) returns (GetContractVersionResponse);
  rpc DiscoverCommandTools(DiscoverCommandToolsRequest) returns (DiscoverCommandToolsResponse);
  rpc DiscoverResources(DiscoverResourcesRequest) returns (DiscoverResourcesResponse);
}

service CommandService {
  rpc Execute(ExecuteCommandRequest) returns (ExecuteCommandResponse);
  rpc GetOutcome(GetOutcomeRequest) returns (GetOutcomeResponse);
}

service QueryService {
  rpc GetEntity(GetEntityRequest) returns (GetEntityResponse);
  rpc ScanIndex(ScanIndexRequest) returns (ScanIndexResponse);
  rpc QueryProjection(QueryProjectionRequest) returns (QueryProjectionResponse);
  rpc GetProjectionStatus(GetProjectionStatusRequest) returns (GetProjectionStatusResponse);
}

service CommitService {
  rpc GetCommit(GetCommitRequest) returns (GetCommitResponse);
  rpc ScanCommits(ScanCommitsRequest) returns (ScanCommitsResponse);
  rpc SubscribeCommits(SubscribeCommitsRequest) returns (stream CommitNotification);
  rpc TraceProvenance(TraceProvenanceRequest) returns (TraceProvenanceResponse);
}

service AdminService {
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc Stats(StatsRequest) returns (StatsResponse);
  rpc CreateCapability(CreateCapabilityRequest) returns (CreateCapabilityResponse);
  rpc RevokeCapability(RevokeCapabilityRequest) returns (RevokeCapabilityResponse);
  rpc ListPendingOutboxDeliveries(ListPendingOutboxDeliveriesRequest) returns (ListPendingOutboxDeliveriesResponse);
  rpc CreateOfflineBackup(CreateOfflineBackupRequest) returns (CreateOfflineBackupResponse);
  rpc RestoreOfflineBackup(RestoreOfflineBackupRequest) returns (RestoreOfflineBackupResponse);
  rpc GetOfflineMaintenanceOperation(GetOfflineMaintenanceOperationRequest) returns (GetOfflineMaintenanceOperationResponse);
}
```

The package remains `riffdb.v1`. The result is exactly five services and 25
RPCs; `CommandService` is unchanged and `SubscribeCommits` remains the sole
server-streaming RPC. Existing methods remain first and retain their descriptor
order. The six added methods, their exact message/field/oneof/enum tags, the
new `discovery.proto` source, and the additive import/method order in
`services.proto` are the byte-for-byte registry accepted in ADR-0040. Every new
request has required `bytes request_id = 1`; Health remains the sole existing
optional-ID exception. Unknown fields are ignored but never relayed, and every
selector/result oneof requires exactly one known branch.

ADR-0050 adds only the final three AdminService methods after the exact
ADR-0040 order. WP-155 owns their exact message fields, tags, structural
validation, descriptor/wire fixtures, and checked
`OfflineMaintenanceOperationId`, `BackupNameV1`, and
`ALLOW_REPLACE_NONEMPTY_TARGET` mappings. They do not extend the durable
`ServiceOperationV1` or capability-permission registries and are not MCP
operations.

WP-130 proves `QueryProjection` wire mapping and public-client behavior against an injected API-neutral service stub. WP-170 supplies real projection semantics, WP-185 composes them into `riffdbd`, and WP-200 supplies final public gRPC evidence; the presence of the RPC in the P1 protocol does not move projection implementation into P1.

## 11.3 Execute response envelope

```protobuf
message ExecuteCommandRequest {
  bytes request_id = 1; // UUID bytes; distinct from command input
  string command_name = 2;
  optional uint64 expected_contract_version = 3;
  Value input = 4; // includes the contract-declared idempotency_key field
}

message ExecuteCommandResponse {
  enum CompletionStatus {
    COMPLETION_STATUS_UNSPECIFIED = 0;
    COMMITTED = 1;
    REPLAYED = 2;
    EXECUTED_READ_ONLY = 3;
  }

  CompletionStatus status = 1;
  uint64 commit_sequence = 2;
  uint64 contract_version = 3;
  bytes plan_hash = 4;
  string outcome_type = 5;
  Value outcome = 6;
  string provenance_uri = 7;
  string durability_mode = 8;
  optional string outcome_uri = 9;
}

message Value {
  oneof kind {
    NullValue null_value = 1;
    bool bool_value = 2;
    sint64 i64_value = 3;
    uint64 u64_value = 4;
    Decimal decimal_value = 5;
    Money money_value = 6;
    string string_value = 7;
    bytes bytes_value = 8;
    bytes uuid_value = 9;       // exactly 16 bytes
    Date date_value = 10;
    Timestamp timestamp_value = 11;
    EnumValue enum_value = 12;
    ValueList list_value = 13;
    ValueRecord record_value = 14;
  }
}

enum NullValue { NULL_VALUE = 0; }

message Decimal {
  bytes coefficient_twos_complement = 1; // canonical minimal big-endian encoding
  uint32 scale = 2;
  optional uint32 precision = 3; // assertion on input; mandatory from a new server
}

message Money {
  string currency = 1;
  Decimal amount = 2;
}

message Date { sint32 days_since_unix_epoch = 1; }
message Timestamp { sint64 seconds = 1; uint32 nanos = 2; }
message EnumValue { uint32 type_id = 1; uint32 variant_id = 2; string name = 3; }
message ValueList { repeated Value values = 1; }
message ValueField { optional uint32 field_id = 1; string name = 2; Value value = 3; }
message ValueRecord { repeated ValueField fields = 1; }
```

For `EXECUTED_READ_ONLY`, `commit_sequence` is the wire sentinel zero,
`provenance_uri` and `durability_mode` are empty, and `outcome_uri` is absent.
Adapters map these to semantic absence and never construct `CommitSequence(0)`,
a provenance identity, durability mode, or locator. `COMMITTED` and `REPLAYED`
require their original nonzero sequence and complete terminal fields. A WP-137
server MUST emit a canonical `outcome_uri` bound to the exact durable identity
for every committed/replayed result; failure to mint it prevents release. An
ordinary upgraded client may accept absence from a pre-WP-137 server as the
legacy shape, but a present value must be canonical and status-consistent.
Unknown status values and every status/field inconsistency reject. Read-only
status gives no durable replay or outcome-recovery promise.

`GetOutcomeRequest` additively owns `optional string outcome_uri = 5`. When
present, legacy `contract_lineage = 2`, `command_name = 3`, and
`idempotency_key = 4` MUST all be empty; when absent, their existing nonempty
raw-key semantics remain unchanged. The service converts it only to the checked
`ResolveCommandOutcomeRequest::Locator(OutcomeResourceLocator)` selector. The
locator path shares the existing operation and audit tag, initial existence-
blind and terminal disclosure authorization, and one authoritative point-lookup
port. It does not expose a raw key, key material, HMAC operation, scan, active-
catalog substitution, or adapter-constructible storage key. A WP-137 server
returns a found raw-key result with a locator minted from matched durable digest
evidence; a found locator result returns a byte-for-byte equal URI.

The custom `Value` family is required; `google.protobuf.Struct` is forbidden at exact business-value boundaries because it cannot preserve all signed/unsigned integer and decimal values. Lists, records, strings, and bytes inherit compiled or protocol bounds. Record fields MUST be unique and canonically ordered by stable field ID where available, then by UTF-8 name. Decimal coefficient, scale, optional precision, currency, UUID, date, and timestamp encodings MUST be validated and canonical before hashing or persistence.

Decimal precision is optional only for compatibility and pre-schema submission:
it is in `1..=38` when present, scale may not exceed it, and service
materialization requires supplied precision/scale to equal the selected schema.
A new server emits precision for every decimal and money amount. General new
clients may accept legacy absence, but MCP stdio fails closed if it would
otherwise have to invent precision.

`EnumValue` accepts either both nonzero stable IDs with an optional redundant
exact name, or both IDs zero with one nonempty exact source name. Mixed zero and
nonzero IDs and an all-empty form reject structurally. The latter form is
resolved only within the enum type required at that exact position in the
selected operation schema. Resolution is case-sensitive and performs no
normalization.

The service retains a private bounded schema-bound presentation plan inside
each validated `DeclaredOutcomeView`. Execute and found GetOutcome public
responses use it to populate both stable IDs and exact field/enum names; hosted
MCP consumes the same view. Generic fixed tagged-value conversion remains
ID-based. No adapter may infer names from record position or rediscover a
then-active bundle.

ADR-0046 also appends `ContractDescriptor.compatibility = 6` as a
`ContractCompatibilitySummary`: optional paired parent version/hash, a closed
three-class overall result, and at most 20 strictly code-ordered nonzero
`CompatibilityCode` counts totaling at most 4,096. A new server always emits
the summary; genesis has no parent and no counts. Full affected paths remain in
the authoritative bundle report and are not copied into the public descriptor.

`riffdb-proto` is the single owner of all checked-in `.proto` sources and
generated compatibility fixtures. WP-020 freezes package/version rules, common
exact values and errors, five service and 16 RPC names, phase-zero shells,
bounds, generation, and `StoredEnvelope`; it is not reopened by later semantic
owners. The focused WP-010 follow-up may add only ADR-0012's already reviewed
`CommandExecutionFailed` kind, detail, closed code, mapper, preflight, descriptor,
and golden fixtures so the domain registry remains exhaustively compilable. It
may add no other public or durable symbol. Formal WP-065, after WP-060, owns all durable semantic-record messages,
descriptors, hashes, goldens, wire validation, historical registration, and the
checked storage-owned mapping bridge before WP-070 persistence. Formal WP-127,
after WP-120, completes public messages, descriptors, hashes, goldens, and wire
validation before WP-130 conversion. WP-127 never depends on `riffdb-service`
and owns no semantic conversion; WP-130 owns total service-to-wire conversion.
No package may invent a competing schema, persist an ad hoc encoding, or guess
field numbers before its proto-owner package.

WP-137 is the sole narrow additive public-protocol owner after WP-127. It owns
ADR-0040's six RPCs/messages and two outcome-locator fields plus ADR-0046's
decimal-precision field, contract-compatibility summary, schema-bound public
outcome presentation, exact descriptor/compatibility fixtures, and total
conversions. The shared service owns the
immutable canonical Draft 2020-12 sources
`riffdb.command-operation-envelope/v1` and
`riffdb.command-get-outcome-result/v1`; each is at most 65,536 bytes and the
complete artifact charges total at most 131,584 bytes. Full command discovery
carries both IDs, hashes, dialects, and exact canonical bodies, while compact or
unchanged discovery carries only their ordered identities in the required
fence. Any source/body/hash/identity change requires a new version and human
compatibility review. These schemas are not compiler artifacts and do not alter
a bundle, bundle hash, plan hash, canonical input hash, or declared-outcome
union.

Before WP-130 exposes the ADR-0012 error, the public error registry additively
includes `PUBLIC_ERROR_KIND_COMMAND_EXECUTION_FAILED = 9`, stable code
`command_execution_failed`, static message `command execution failed`, recovery
`CONTACT_OPERATOR`, and required `CommandExecutionFailureDetails` in
`PublicError.execution_failure = 8`. Its required closed code is arithmetic
fault `1` or resource limit `2`; zero and unknown values reject. gRPC maps it to
`FAILED_PRECONDITION`. No existing error value or field is renumbered.

For every API-neutral `PublicError`, WP-130 MUST encode the exact checked
`riffdb.v1.PublicError` bytes, enforce `MAX_PUBLIC_ERROR_BYTES = 16 KiB`, and pass
those bytes directly to Tonic `Status::with_details` as
`grpc-status-details-bin`. It MUST NOT wrap them in `google.rpc.Status`, `Any`, or
a second custom envelope. The gRPC status is derived only from `ErrorClass`:
`InvalidArgument -> INVALID_ARGUMENT`, `Conflict -> ALREADY_EXISTS`,
`PermissionDenied -> PERMISSION_DENIED`,
`DeadlineExceeded -> DEADLINE_EXCEEDED`,
`FailedPrecondition -> FAILED_PRECONDITION`, `Unavailable -> UNAVAILABLE`,
`Uncertain -> UNKNOWN`, and `Internal -> INTERNAL`. `grpc-message` MUST equal the
registry-owned static safe message and MUST NOT contain an internal source,
arbitrary diagnostic, incident narrative, or serialized error.

The Rust client MUST accept a non-OK API-neutral response as a `PublicError` only
when details are present, within 16 KiB, valid under the checked public-error
decoder, and consistent with both the canonical status and static safe message.
Missing, malformed, oversized, unknown, status-inconsistent, or
message-inconsistent details are a closed typed protocol failure; the client MUST
NOT infer a public error or retry action from `grpc-message` alone. The existing
pre-authentication `UNAUTHENTICATED` framing exception remains a distinct closed
transport error before an API-neutral `PublicError` exists and carries no
fabricated public-error detail.

`AdminService.CreateCapability` has a closed create mode: unspecified `0`,
normal `1`, bootstrap `2`; zero and unknown values reject. Normal creation uses
ordinary bearer authentication and may return the one newly issued token only
after durable success. Bootstrap is loopback gRPC only, never MCP, and carries
its retained canonical token in exactly one sensitive binary metadata entry
`riffdb-bootstrap-token-bin`; the request message contains no raw token. Normal
creation rejects bootstrap metadata, and bootstrap rejects ordinary
`authorization` metadata. The service-owned closed results and exact public
fields are frozen by WP-127 before WP-130 implements them.

## 11.4 Limits

- Maximum unary request size: configurable; default 1 MiB.
- Maximum unary response size: configurable; default 4 MiB.
- Command input values are bounded by contract types.
- Scan operations require explicit limit and opaque cursor.
- Server deadlines cap lock wait, command evaluation, commit queue wait, and projection wait separately.
- Compression is disabled for every POC gRPC message, including contract
  bundles and scans.

## 11.5 Rust SDK

The POC MUST provide:

- A generic client capable of invoking commands by name and dynamic typed value.
- Generated Rust input structs and outcome enums for the example contract.
- Automatic idempotent retry on safe transport failures using the same canonical
  command input and idempotency key with a fresh `RequestId` for each submission.
- An explicit `OutcomeUnknown` client error when automatic resolution cannot complete.
- `wait_for_projection(sequence)` helpers.
- Trace-context propagation.

WP-137 additionally exposes checked unary methods for exactly
`get_contract_version`, `discover_command_tools`, `discover_resources`,
`get_projection_status`, `trace_provenance`, and
`list_pending_outbox_deliveries`. Each consumes its exact generated request plus
`&CallMetadata`, validates outbound/inbound messages and operation-specific
request/response relations, and returns only its exact checked response or
`ClientError`. The crate root re-exports the single `riffdb-errors` public-error
owner types named by ADR-0040; it does not create a wire view, duplicate error
registry, or expose peer/Tonic text as retry authority.

The only automatic retry helpers are `execute_with_retry`,
`create_capability_with_retry`, and
`create_bootstrap_capability_with_retry`. They accept checked private-field
operation templates plus an explicit attempt budget, obtain a fresh UUIDv7
`RequestId` before every submission including the first, preserve every semantic
identity/body byte, and consume only checked recovery actions or the private
details-free transport-unavailable classification. They expose no generic
callback/arbitrary-RPC retry, status-only predicate, default budget, sleep, or
jitter. Once any attempt is uncertain, exhaustion or a later unresubmittable
failure returns the existing `OutcomeUnknown`; normal capability replay may
terminally return `AlreadyCreatedTokenUnavailable` and never creates a
replacement.

The public client also exposes exactly
`load_protected_bearer_credential(&Path) -> Result<BearerCredential,
BearerCredentialFileError>` and
`BearerCredential::has_same_presentation(&self, &BearerCredential) -> bool`.
The closed error variants are `UnsupportedPlatform`, `ProtectedFileRejected`,
and `InvalidPresentation`, with fixed redacted text and no source/path. On Linux
the loader implements ADR-0041's exact bounded `/proc/self/status`, final
symlink, opened-file device/inode/owner/mode, 43-byte/no-trailing-byte, and
zeroized-buffer procedure; non-Linux rejects before credential read. It performs
only presentation validation, exposes no raw bytes, does not base64-decode,
hash, authenticate, or consult capability state, and is the sole protected
normal-file loader used by CLI and MCP stdio.

Generated SDK code MUST not contain database correctness logic. It validates ergonomics and shapes data; the server remains authoritative.

---
# 12. Native Model Context Protocol interface

## 12.1 Design position

MCP is a first-class product interface because the database is intended for agent-driven development and operation. The MCP layer is generated from the same active contract bundle and uses the same service, authorization, policy, command runtime, and provenance paths as gRPC.

The POC baseline is MCP protocol version **2025-11-25** and the official Rust
SDK `rmcp` exactly 2.2.0 under ADR-0008's reviewed dependency graph. MCP
protocol and SDK use MUST be isolated behind `riffdb-api-mcp` so future protocol
revisions do not leak into compiler, runtime, or storage crates.

## 12.2 Transports

### Local stdio

`riffdb-mcp` provides a local stdio MCP server suitable for IDEs and local coding agents.

```text
MCP host
  -> launches riffdb-mcp --endpoint http://127.0.0.1:7443
  -> credentials supplied through environment or protected local config
  -> riffdb-mcp invokes the shared RiffDB service through gRPC
```

The stdio process MUST write protocol messages only to stdout. Diagnostics go to stderr.
It installs the `riffdb-api-mcp` hard tracing predicate as the final outer
global filter so exact target `rmcp` and every `rmcp::` descendant are
discarded regardless of user directives or reload state. Formatting explicitly
uses stderr with ANSI disabled, and subscriber installation failure stops the
process before MCP service.

Production stdio is an ordinary public client and links no `riffdb-service`,
auth, policy, catalog, runtime, commit, server, or storage crate. It enables only
the `riffdb-api-mcp` `stdio` feature and never the gRPC server feature. Its only
recognized environment names are `RIFFDB_MCP_CONFIG`, `RIFFDB_MCP_ENDPOINT`,
`RIFFDB_MCP_CAPABILITY_TOKEN`, and `RIFFDB_MCP_CREDENTIAL_FILE`; its only
configuration flags are `--config` and `--endpoint`. It performs no ambient path
discovery. Configuration, endpoint, credential exclusivity, protected-file
loading, 4,096-byte path, 65,536-byte TOML, 512-byte exact literal-loopback
endpoint, and exact 43-byte environment-presentation rules are those frozen by
ADR-0008. A credential never appears in argv or TOML, and the bridge uses the
public-client protected loader rather than `riffdb-auth`.

### Streamable HTTP

`riffdbd` exposes a native `/mcp` Streamable HTTP endpoint.

- POC: disabled by default, loopback binding only, opaque development capability token.
- MVP: remote use over TLS, OAuth 2.1 protected-resource behavior, audience-bound access tokens, tenant policy, rate limits, and audit.

The POC route is exactly `/mcp`; no trailing slash, alternate prefix, wildcard,
query-selected route, remote bind, TLS, or OAuth behavior is accepted. The
configured protected-resource URI is absolute lowercase HTTP with a loopback IP
literal, explicit port, exact path, and no user information, query, or fragment.
Every request must have one byte-equal effective authority and, when `Origin` is
present, one exact member of the bounded configured loopback-origin allowlist.
The configured audience is never synthesized from `Host`, forwarding headers,
or origin.

Every POST, GET, or DELETE carries exactly one canonical `Authorization` value:
case-sensitive `Bearer ` plus the exact 43-byte token. The adapter strips the
scheme, calls only the auth-owned `CredentialAuthenticator` with trusted
database/environment/audience, removes the header before rmcp, and owns no token
decoder or auth cache. For the ordered adapter handoff it may copy the
bearer-stripped slice into auth-owned `RetainedOpaqueCredential`, whose
constructor accepts any length through 43 without syntax validation and whose
sole public data accessor `borrow()` returns `OpaqueCredential<'_>`. It passes
that borrow only to `CredentialAuthenticator`; it never stores the retained
value or borrow in session state, exposes raw bytes, or treats construction as
authentication. A live SSE response handler owns exactly one retained value for
its lifetime, uses it only to freshly authenticate each emitted notification,
and releases it on close; it caches no authentication or authorization result.
Stateful rmcp mode is mandatory; stateless
`serve_directly` is forbidden. The private RiffDB `SessionManager` reserves one
validated SDK-produced ID by insert-if-absent before worker start, counts Pending
and Active states against 128, never retries/evicts/overwrites, holds no map guard
across await, and promotes the exact initialized ID to an Active
`CapabilityId`/clock binding. Every later request reauthenticates and must match
that binding. Invalid, collided, abandoned, expired, deleted, cancelled, or
failed sessions release state/worker exactly once and reveal no existence detail.
Session idle and lifetime are 300 and 900 seconds; SDK keepalive and SSE retry
are both disabled.

After authentication, hosted HTTP obtains a fresh checked `RequestId` from the
injected `riffdb-api-mcp` consumer port and constructs service context only
through `RequestContext::from_authenticated_mcp_http`. Source failure stops
before service invocation or durable administration audit. The constructor
fixes MCP HTTP ingress and the empty v1 claim set. WP-140 supplies no trace
context unless server composition already provides an explicitly trusted
carrier; MCP identifiers, credentials, headers, and tool values are never
promoted to request, agent-session, claim, or trace identity.

Validation order is route/method framing, Origin, effective authority,
pre-authentication rate limit and credential, protocol/session headers, bounded
body, then MCP dispatch. Initialization carries no session header; every later
POST/GET/DELETE carries exactly one canonical bound header. Origin failure is
403, authority failure 421, missing/malformed credential 401 with a generic
Bearer challenge, and absent/malformed/unknown/mismatched post-init session 404;
all are existence-blind, `Cache-Control: no-store`, and release no JSON-RPC body
or protected work. ADR-0008's exact header duplication, whitespace, ASCII,
allowlist, and response rules apply.

`MCP-010` Stdio and Streamable HTTP MUST expose equivalent authorized tools and resources for the same principal and active contract.

`MCP-011` Transport-specific authentication MUST call the same auth-owned
`CredentialAuthenticator`, produce the same privately constructed
`AuthenticatedPrincipal`, and enter the same service-owned authorization and
obligation path. MCP adapters MUST NOT construct an actor or capability
decision. Streamable HTTP may only bound and copy the bearer-stripped
presentation into auth-owned `RetainedOpaqueCredential` and pass its
`OpaqueCredential` borrow to that authenticator; helper construction is not
credential syntax validation, authentication evidence, or authorization.

## 12.3 Advertised capabilities

### POC server capabilities

- Exactly `tools: { listChanged: true }`.
- Exactly `resources: { subscribe: true, listChanged: true }`.

No other server capability is advertised. Optional title, description, icons,
website URL, instructions, prompts, logging, completions, and experimental
capabilities are absent rather than empty or false-valued extensions. Progress
and cancellation remain protocol behaviors under Sections 12.9 and 9.7, not
extra advertised server capability fields.

### Explicitly deferred MCP capabilities

- Client sampling initiated by the database.
- Roots.
- Elicitation.
- Experimental or extension-based task execution.
- MCP Apps.
- Prompts in the POC.

The adapter MUST negotiate capabilities and MUST NOT call client features that were not declared.

## 12.4 Dynamic command tools

Every command authorized for the connected principal is exposed as an MCP tool generated from its compiled plan.

### Naming

```text
riffdb.cmd.<contract-segment>.<command-segment>
```

Example:

```text
riffdb.cmd.legalspend.allocatebudget
```

- Each segment is derived from the exact source contract or command identifier
  by mapping ASCII `A` through `Z` to ASCII lowercase. Existing lowercase ASCII
  letters, digits, and underscores are preserved byte for byte; no other
  normalization, separator insertion, trimming, escaping, or Unicode case
  conversion occurs.
- After mapping, each segment MUST match `[a-z][a-z0-9_]*`. An empty segment, a
  source identifier beginning with an underscore, or any other invalid segment
  rejects compilation.
- The complete ASCII name, including `riffdb.cmd.` and both separators, MUST be
  at most 128 bytes inclusive. A 129-byte name rejects and is never truncated.
- WP-040 MUST reject every complete-name normalization collision and MUST NOT
  append an ordinal, hash, version, or other suffix. It emits the checked,
  versioned, deterministically ordered command-name registry as a compiler
  artifact covered by the contract bundle's canonical encoding and hashes.
- WP-050 MUST independently revalidate the compiled registry during activation,
  including derivation, completeness, stable-ID binding, canonical ordering,
  length, segment validity, and complete-name uniqueness. It never repairs or
  substitutes a public name.
- The service exposes policy-filtered descriptors carrying the compiled name,
  and the MCP adapter consumes that name verbatim for discovery and invocation;
  neither layer reimplements normalization.
- Contract deployment that adds, removes, or changes visible command tools emits `notifications/tools/list_changed` when negotiated.

ADR-0020 owns only naming. Accepted ADR-0008 separately freezes resource URI
encoding, HTTP audience, stdio-over-gRPC topology, cursor presentation, and the
associated compatibility boundaries in Sections 12.2 through 12.12; the future
WP-140 fixed-schema and resource-registry bytes still require their explicit
interface-only human checkpoint.

### Generated tool definition

Each dynamic tool consumes the compiler-owned exact name, input artifact, and
declared-outcome union. The adapter advertises that input artifact unchanged and
mechanically composes the exact declared union into the `outcome` position of
the service-owned `riffdb.command-operation-envelope/v1` source. The resulting
closed Draft 2020-12 output has three ordered status branches: `committed`,
`replayed`, and `executed_read_only`. Every branch requires `status`,
`commit_sequence`, `contract_version`, `plan_hash`, `outcome`,
`provenance_uri`, `durability_mode`, and `outcome_uri`; the four durability-
specific fields are canonical non-null values for committed/replayed and JSON
null for read-only. Contract version is a nonzero JSON integer, plan hash is 64
lowercase hexadecimal characters, and `outcome` matches the inserted exact
compiler union. The composer does not maintain a second command-specific schema
source or modify a bundle/hash.

The fixed `riffdb.command.get_outcome` tool instead always advertises the
invocation-independent `riffdb.command-get-outcome-result/v1` schema. It has
exactly a closed `{ "status": "not_found" }` branch and a closed replay branch
whose ordered fields are `status`, `commit_sequence`, `contract_version`,
`plan_hash`, `outcome_type`, tagged structural `outcome`, `provenance_uri`,
`durability_mode`, and `outcome_uri`. It never substitutes a selected command's
union after seeing invocation arguments.

The MCP tagged structural Value is a closed object discriminated by lowercase
`kind`, with exact variants `null`, `bool`, `i64`, `u64`, `decimal`, `money`,
`string`, `bytes`, `timestamp`, `date`, `uuid`, `enum`, `list`, and `record`.
Integers that may lose JSON precision use canonical decimal strings; bytes use
canonical padded standard base64; UUID uses lowercase hyphenated text; decimal
and money carry checked precision/scale/coefficient; lists contain at most
65,535 ordered values; records contain at most 65,535 strictly increasing
nonzero `field_id`/value pairs; nesting depth is at most 32; and complete
encoded/scalar bounds remain authoritative even when JSON Schema cannot express
them. No floating point, display alias, map, lossy integer, alternate base64, or
unknown property is accepted.

Natural dynamic command input enums are exact compiler-schema strings. The
common MCP converter represents them as ADR-0046's name-only public submitted
form; the shared service resolves them under the selected schema. Dynamic
declared outcomes are rendered from the service-owned schema-bound
`DeclaredOutcomeView`, so record properties and enum strings use exact
historical compiler names without an adapter-side catalog lookup. Hosted and
stdio adapters use the same common renderer.

`MCP-020` Generated schemas MUST come from the compiled contract bundle, not hand-maintained MCP code.

`MCP-021` Structured tool results MUST conform to `outputSchema` and SHOULD also include a compact JSON text content block for client compatibility.

`MCP-022` Declared business outcomes MUST return `isError=false`, including rejections such as `InsufficientBudget`.

`MCP-023` Malformed arguments, authorization denials, idempotency-key misuse, unavailable server, and internal failures MUST be represented as actionable tool execution errors or protocol errors according to the failure layer.

`MCP-024` Mutating command tools MUST require a caller-supplied idempotency key. The MCP adapter MUST NOT silently invent a new key on retry.

The MCP protocol request identifier and the service-generated outer `RequestId` are transport metadata, not the contract's caller-supplied `idempotency_key`. MCP invocation creates a fresh outer `RequestId` while preserving the caller key in the validated tool arguments.

## 12.5 Fixed administrative and read tools

| Tool | Risk class | POC | Purpose |
|---|---|---|---|
| `riffdb.contract.validate` | Read-only compute | Required | Compile source and return diagnostics and plan summary without deployment. |
| `riffdb.contract.get_active` | Read-only | Required | Return active contract metadata and resource link. |
| `riffdb.contract.explain_command` | Read-only compute | Required | Explain partition, conflict, reads, writes, invariants, outcomes, and cost class. |
| `riffdb.contract.deploy` | Administrative mutation | Required, dev capability only | Deploy a validated bundle using expected active version and approval metadata. |
| `riffdb.command.get_outcome` | Read-only | Required | Resolve an uncertain command by principal scope, command, and idempotency key. |
| `riffdb.entity.get` | Read-only data | Required | Fetch one entity with policy filtering. |
| `riffdb.entity.scan_index` | Bounded read | Required after index support | Scan a declared index with limit and opaque cursor. |
| `riffdb.commit.get` | Administrative read | Required | Fetch one commit record with redaction. |
| `riffdb.commit.scan` | Bounded administrative read | Required | Scan commits with cursor, principal policy, and limit. |
| `riffdb.provenance.trace` | Administrative read | Required | Return causal metadata and linked resources. |
| `riffdb.projection.query` | Read-only data | Required | Query a projection with optional `after_sequence` wait. |
| `riffdb.projection.status` | Read-only | Required | Return frontier and lifecycle state. |
| `riffdb.outbox.list_pending` | Administrative read | Required | Inspect pending deliveries without payload fields the caller cannot read. |
| `riffdb.server.health` | Read-only | Required | Return readiness, active contract, last commit, and degraded components. |

Administrative tools MUST be absent from `tools/list` when the session lacks permission, rather than merely failing after selection.

These are exactly the 14 fixed tools in their displayed order and correspond to
ADR-0040 `FixedToolKind` tags 1 through 14. WP-140 owns one versioned
`riffdb-api-mcp` registry, not runtime reflection, Protobuf debug output, or
`schemars` generation. The complete operation/fixed manifest has 29 unique
artifacts: the generic command envelope, the reused GetOutcome result, 14 fixed
inputs, and 13 new fixed results. Each fixed artifact is at most 65,536 bytes
and the 27 new artifacts together are at most 1,048,576 bytes. The full
`tools/list` ledger is exactly 2,621,440 bytes of service discovery,
1,048,576 bytes of local fixed schemas, and 524,288 bytes of all remaining MCP
keys/text/annotations/JSON-RPC/link/framing, totaling 4,194,304 bytes. WP-140's
interface-only PR must separately receive human acceptance for the exact 27
source bytes, IDs/hashes, mappings, converters, and goldens before presentation
implementation; accepting ADR-0008 did not pre-accept those future fixtures.

## 12.6 Resource model

MCP resources provide application-controlled context. Resources are redacted by the current principal and can be returned as links from tools.

### Resource URIs

| URI template | MIME type | Contents |
|---|---|---|
| `riffdb://contract/active` | `application/json` | Active contract metadata and links |
| `riffdb://contract/<lineage>/<version>` | `application/json` | Bundle summary, source hash, plan hash, compatibility metadata |
| `riffdb://entity/<lineage>/<entity-id>/schema` | `application/schema+json` | Entity JSON Schema and field policy metadata |
| `riffdb://command/<lineage>/<command-id>/plan` | `application/json` | Explain plan and generated schemas |
| `riffdb://command/<lineage>/<command-id>/docs` | `text/markdown` | Generated command documentation and examples |
| `riffdb://outcome/<principal>/<lineage>/<command-id>/<tool-name>/<key-hash>` | `application/json` | Authorized persisted outcome for uncertainty recovery |
| `riffdb://commit/<sequence>` | `application/json` | Redacted commit record |
| `riffdb://provenance/<provenance-id>` | `application/json` | Actor, agent session, source, approval, contract, and affected aggregates |
| `riffdb://projection/<lineage>/<projection-id>/status` | `application/json` | Lifecycle, frontier, lag, and error state |
| `riffdb://server/health` | `application/json` | Health and readiness information |

The active-contract JSON includes
`links.contract_version` for its exact canonical version URI. The immutable
version JSON includes ADR-0046's bounded compatibility summary, with JSON
`null` genesis parent or an exact parent version/hash object. Command-plan JSON
includes the complete checked explanation and the exact compiler-owned input
and outcome schema objects returned by fenced `ExplainCommand`.

Command-documentation Markdown contains only escaped factual descriptor/explain
fields, the compiler-rendered explanation, one deterministic minimal input
example, and one deterministic example per declared-outcome schema branch. The
common generator supports only the accepted compiler JSON Schema subset,
validates every generated example with the same Draft 2020-12 validator before
release, and fails the whole bounded resource on unsupported or unsatisfied
shape. For an idempotent-mutation execution class it also includes the fixed
safety notice: `Cancellation after command submission may not prevent commit.
Resolve an uncertain result with the same idempotency key or the returned
outcome URI.` It invents no business prose.

Projection-status JSON always includes `lag`: a canonical unsigned-decimal
string measuring published frontier distance to authoritative head, treating
`before_first` as ordinal zero, or JSON `null` when no published frontier
exists. Lag is derived presentation and never a second stored frontier.

Concrete active-contract, contract-version, entity-schema, command-plan,
command-documentation, exact commit, exact provenance, projection-status, and
health descriptors appear only in `resources/list`. The outcome descriptor
appears only in `resources/templates/list` as
`riffdb://outcome/{principal}/<lineage>/<command-id>/<tool-name>/{key_hash}`;
commit/provenance class descriptors appear only there as
`riffdb://commit/{sequence}` and
`riffdb://provenance/{provenance_id}`. A descriptor never appears on both
surfaces, a template grants no content authority, and an expanded URI must pass
the same canonical parser. `resources/list` calls full `DiscoverResources` with
limit 500 and kind `Concrete`; `resources/templates/list` uses kind `Template`;
compact watchers use kind `All`. The exact 16-byte service cursor is presented
unchanged except as 32 lowercase hexadecimal characters and cannot cross kind,
representation, limit, or operation.

Lineage/principal bytes are exact checked UTF-8, with only non-unreserved bytes
percent-encoded using uppercase two-digit escapes; parsers reject lowercase or
malformed escapes, escaped unreserved bytes, invalid UTF-8, and normalization.
Numeric IDs/version/sequence are nonzero unsigned canonical decimal with no
sign or leading zero. Stable entity/command/projection IDs, rather than display
names, bind targets. Tool name is the exact compiler-owned ADR-0020 ASCII name.
Every locator has lowercase scheme/authority, the exact segment count, and no
port, user info, empty segment, trailing slash, query, or fragment. Producers
emit only canonical text; parsers never repair it. Non-provenance locators are
at most 2,048 bytes, with the outcome locator's tighter exact maximum 1,745
bytes; the ADR-0024 provenance bound remains unchanged.

`key-hash` is exactly 50 characters: canonical unpadded base64url of
`u8 digest_scheme || u32_be(digest_key_id) || digest[32]`, with scheme 1 and a
nonzero key ID. It is sensitive correlation data but not the raw idempotency key
or a secret. Only the shared service mints a checked locator after durable
identity resolution and terminal disclosure authorization. A locator read
reconstructs the identity from trusted process context, authenticated principal,
Global tenant, checked lineage/command ID, and digest tuple, performs one point
lookup, then checks the stored historical plan's exact tool name before release.
No adapter computes HMAC, receives digest-key material, scans records, or treats
the URI as authority.

`MCP-030` Resource list and content MUST be filtered by capability and field policy.

`MCP-031` Resource subscriptions SHOULD be implemented for active contract, command plan, projection status, and server health resources.

`MCP-032` Contract deployment MUST trigger resource list or update notifications when negotiated.

`MCP-033` Resource reads MUST enforce bounded payload sizes and MAY return resource links to paginated detail instead of embedding large data.

Every content read is service-mediated and reruns current authorization, audit,
obligations, bounds, and redaction. Active contract uses `GetActiveContract`;
version uses exact-identity `GetContractVersion`; entity schema uses one exact
fresh full-discovery match; outcome uses locator-form
`ResolveCommandOutcome`; commit/provenance/projection/health use respectively
`GetCommit`, `TraceProvenance`, `GetProjectionStatus`, and `Health`. A command
plan/documentation read first scans compact resources to exact end (at most
three limit-500 calls and 1,024 accepted items), obtains the unique self-
contained descriptor and fence, calls `ExplainCommand` under its exact
lineage/version/source name, checks stable command identity, then requires a
conditional `catalog_unchanged` result under that same fence before content
release. It never joins through tool discovery or substitutes the active
version. WP-140's separately reviewed resource-registry fixture freezes every
descriptor-to-surface/URI/MIME/converter/subscription mapping and result golden.

## 12.7 Prompt templates

Prompts are user-controlled MCP primitives and are not part of the POC
capability or WP-140 acceptance. A later reviewed stage MAY consider:

| Prompt | Purpose |
|---|---|
| `review_contract_change` | Ask an agent to inspect validation, compatibility, locality, invariant, and projection impact before deployment. |
| `diagnose_command_result` | Explain a typed business outcome or execution failure using linked command and provenance resources. |
| `prepare_safe_migration` | Produce a migration plan constrained by current contract compatibility and policy. |

Prompt templates MUST not grant capabilities or bypass approval. They only assemble context and instructions.

## 12.8 Authorization and tool visibility

The MCP server calculates a session-specific catalog:

```text
active contract tools
  intersect principal command allowlist
  intersect tenant/partition scope
  intersect environment policy
  intersect approval state
  minus hidden administrative operations
```

Tool visibility is not the authorization boundary. Every invocation MUST repeat authorization using current capability and policy state.

After initialization, list-change notifications are emitted when the
corresponding already-authorized visible fingerprint inventory changes; they do
not depend on a client capability bit. Resource updates are emitted only for an
explicitly subscribed exact URI. The subscribable v1 set is exactly active
contract, command plan, projection status, and server health, with at most eight
distinct URIs per session. Other resource kinds reject subscription. Repeating
subscribe is idempotent; hidden/revoked resources are removed without exposing
why.

One bounded observer per session runs on injected five-second monotonic ticks
for at most the 900-second session lifetime. It conditionally scans both compact
inventories under the prior common presentation fence. On change, each scan
uses limit 500, reaches exact end in at most three calls for at most 1,024
accepted visible items, and retains only compact MCP-visible fingerprints. An
exact 1,024-item inventory yields 500/500/24. A larger inventory may return up
to 500 items on the third page but is rejected at item 1,025 or any cursor after
item 1,024; no more than 1,024 fingerprints are retained. Cursor/fence/auth
failure discards the entire refresh and restarts only at the next tick. A hidden-
only semantic change produces no notification or count leak.

A 32-permit server-wide semaphore bounds active observation passes; a task that
cannot acquire immediately coalesces one due marker. Per session, delivery
retains at most one tools-list marker, one resources-list marker, and one latest
marker for each of eight subscribed URIs. Output backpressure replaces markers
rather than blocking or growing a queue, and current authorization/content is
rerun before emission. Watcher work never refreshes idle time. Across one
session, watcher work is bounded to 180 ticks, 2,520 logical observer
operations, 8,280 physical service calls, and at most 46 physical calls in one
tick,
at most 1,500 returned/structurally validated compact items per inventory/tick,
and at most 1,024 retained items per inventory/tick. It retains no snapshot,
storage transaction, capability proof, cursor between ticks, or unrestricted
content.

## 12.9 Progress and cancellation

Long-running MCP operations SHOULD send progress when the client provided a progress token:

- Contract compilation across multiple files.
- Compatibility and replay analysis.
- Contract deployment with projection initialization.
- Projection rebuild.
- Branch replay in MVP.

Progress values MUST be monotonic. Progress notifications stop after completion or cancellation.

Progress is sent only when negotiated and the client supplied a token. Values
are finite, nonnegative, monotonic safe integers, come only from service-observed
milestones/bounded completed units, and are capped at 32 notifications per
operation. Request-scoped adapter polling, when genuinely required, permits one
in-flight service call, at most 32 observations, no faster than 100 milliseconds
under an injected clock, and at most 30 seconds overall. Command execution,
deployment, projection waits, and cursor scans do not gain polling loops merely
to populate progress. These request bounds are distinct from the session
observer in Section 12.8.

Cancellation behavior follows Section 9.7. For a mutating command, the MCP adapter MUST document that cancellation after commit submission may not prevent the commit. The idempotency outcome resource is the recovery mechanism.

## 12.10 Pagination

MCP list and scan operations use opaque cursor pagination.

- The API-neutral cursor is exactly 16 opaque server-stored random bytes and MCP
  presents it only as exactly 32 lowercase hexadecimal characters. Prefixes,
  hyphens, uppercase, whitespace, percent encoding, odd length, and base64 reject.
  Adapters encode/decode this spelling only and never inspect state.
- Default page size: 50.
- Maximum page size: 500 for administrative commit scans and lower for entity data when policy requires.
- Cursor state binds principal/capability context, operation, target,
  representation/resource kind, normalized request/policy/catalog fences and
  lower continuation, and expires after the fixed 300-second POC lifetime.
  Clients cannot alter or transfer those bindings.

## 12.11 MCP security requirements

| ID | Requirement |
|---|---|
| `MCP-040` | Validate every tool input against compiled JSON Schema before conversion to internal values. |
| `MCP-041` | Apply authorization after schema validation and again at command execution with current policy. |
| `MCP-042` | Rate-limit by principal, tool, tenant, and source address for HTTP transport. |
| `MCP-043` | Never place raw secrets, capability tokens, or unrestricted PII in tool descriptions, resource metadata, or errors. |
| `MCP-044` | Treat model-provided reason, source commit, and approval references as untrusted claims until validated. |
| `MCP-045` | Bound tool output, resource size, scan count, wait time, and diagnostic count. |
| `MCP-046` | Record every structurally valid, authenticated mutating tool invocation and administrative read after successful `RequestContext` construction in the durable administration audit stream. Malformed or unauthenticated traffic remains bounded transport-security telemetry. |
| `MCP-047` | Stdio credentials MUST come from environment or protected local configuration, not command-line arguments visible in process listings. |
| `MCP-048` | HTTP deployment MUST reject tokens not audience-bound to the canonical MCP resource server. |
| `MCP-049` | The server MUST sanitize user-controlled strings included in Markdown or text results to prevent misleading instruction injection in generated operational summaries. |

The injected monotonic hosted-HTTP rate limiter has two exact default token
buckets: pre-authentication trusted peer-IP burst 32/refill 8 per second, and
post-authentication `(McpHttp, principal, policy tenant, operation or
compiler-owned tool)` burst 16/refill 4 per second. At most 4,096
total buckets are retained; only keys idle at least 300 seconds may be removed,
and capacity exhaustion denies rather than evicts an active key. Monotonic
regression, arithmetic/provider failure, and capacity exhaustion fail closed.
Forwarding headers never select the peer key. Configuration may lower but not
raise these limits.

Production stdio does not recreate the post-authentication bucket from a bearer
presentation or model input because that public-client process owns no
authenticated principal or policy tenant. Its calls remain subject to ordinary
gRPC authentication/admission, shared service authorization and limits, durable
audit, and server-wide resource bounds. `MCP-010` transport equivalence does not
require duplicate ingress throttling at a different trust boundary.

One inbound MCP message is at most 1,048,576 bytes. One complete outbound
JSON-RPC/SSE message, structured result, resource, or error including all
wrappers is at most 4,194,304 bytes. First-party wrappers probe one excess byte
before decode, reject over-limit HTTP `Content-Length` before collection, count
chunked bodies, and fully stage/check a stdio frame before stdout. The private
HTTP session/stream wrappers preflight the typed initialize and every post-ID SSE
frame, and the outer body gate checks actual complete bytes before yielding any
byte of that frame. Compression is disabled. There are at most 128 live MCP
sessions, 256 in-flight requests server-wide, and eight in-flight requests per
session. Rejection allocates no service request, cursor, observer, subscription,
or progress task.

Both binaries apply a non-overridable tracing filter that discards target `rmcp`
and every `rmcp::` descendant because the pinned SDK may log complete protocol
values/session IDs. Only separately constructed bounded safe RiffDB events are
emitted; protocol stdout contains no diagnostics. A framing or rmcp/SSE logging
change is an SDK-upgrade review trigger.

For the POC HTTP transport, the protected resource identity is a configured canonical URI ending in `/mcp`; it MUST NOT be synthesized from the request `Host` header. The stdio bridge connects to `riffdbd` only through gRPC and therefore uses a capability explicitly carrying the configured gRPC audience. Tests MAY inject the API-neutral service in process, but production stdio has no in-process or storage path.

ADR-0009 bootstrap is the sole principal-less durable-audit exception and is
never exposed through MCP. Discovery omissions do not emit one denial record per
hidden item, but invoking a stale or hidden operation repeats current
authorization and emits the normal denied audit record when the authenticated
request is denied. Audit failure never grants permission or releases protected
data.

## 12.12 MCP conformance and compatibility

- Run the official MCP Inspector against stdio and Streamable HTTP variants.
- Add protocol initialization, tool listing, resource listing, schema, pagination, cancellation, and progress integration tests.
- Pin exact `rmcp` 2.2.0 plus the reviewed feature set in the workspace lockfile.
- Maintain an internal adapter trait so a future MCP protocol revision can coexist during migration.
- The POC MUST report exactly protocol `2025-11-25`, implementation name
  `riffdb`, and implementation version equal to the exact `riffdb-api-mcp`
  package version (`0.1.0` for the POC fixture), plus only the two capabilities
  in Section 12.3.

Each transport owns a typed negotiation wrapper outside rmcp `serve_server`.
The wrapper permits the one pre-initialization `ping`; HTTP handles that exact
bounded request itself after route/origin/authority/rate/auth/body checks and
returns HTTP 200 with the same checked ID, `{}` result, `application/json`,
`Cache-Control: no-store`, no session, and no service call. Other methods,
notifications, missing/invalid IDs, parameters, batches, and duplicate fields do
not enter that path. Stdio passes the checked ping through rmcp.

For initialize, exact `2025-11-25` passes unchanged. Any other syntactically
valid offer is replaced only in the typed rmcp input with the fixed private
`riffdb-unsupported` sentinel, which tests prove is unknown to the pinned SDK;
rmcp then selects the configured baseline fallback. Malformed input rejects. An
initialization protocol header may be absent, but when present it occurs once
and equals the original body offer and is rewritten consistently. After
initialization every HTTP POST/GET/DELETE requires exactly one
`MCP-Protocol-Version: 2025-11-25`; absence, duplication, or another value
rejects before rmcp. A syntactically valid different offer therefore receives
the RiffDB baseline, and a client unable to use it disconnects. RiffDB never
advertises or enables another version. The pinned SDK's three older-known and
one newer-known offers, plus an unknown-newer offer, are explicit fixtures; a
changed registry/sentinel/fallback ordering requires review.

---

# 13. Authorization, capabilities, and provenance

## 13.1 Actor context

Transport authentication returns a privately constructible
`AuthenticatedPrincipal` containing the stable principal, actor kind, current
capability identity/revision, audience, tenant scope, and authentication time,
but no raw token or digest material. The adapter combines it with one request ID,
trusted ingress, bounded untrusted provenance claims, deadline/cancellation, and
trusted trace propagation into the service-owned checked `RequestContext`.

The v1 service owns closed constructors for authenticated gRPC and hosted MCP
HTTP. The hosted constructor accepts checked request ID, principal, request
control, and optional trusted trace only; it fixes `McpHttp` ingress and all
five untrusted claims to absent. The accepted MCP surface has no invocation-
claim carrier. A future agent-session or provenance carrier requires separate
review and must never infer identity from MCP session or JSON-RPC state.

Policy decides which claims are valid for one exact operation. A newly admitted
command freezes only stable principal, trusted actor kind,
authorization-resolved tenant scope, and optional validated agent session in
`AdmittedActorContext`. Approved source repository, source commit, reason, and
`ApprovalId` are frozen separately in
`StoredAdmittedProvenanceClaimsV1`. Current capability ID/revision and retry
claims are used for current authorization and service audit but never enter
deterministic runtime or overwrite admission/provenance state.

## 13.2 POC capability tokens

The POC uses opaque random bearer tokens backed by two cross-linked server-side
records and a singleton bootstrap marker. A normal token is exactly 32 bytes
from the approved OS entropy provider outside deterministic command runtime,
encoded as exactly 43 canonical unpadded base64url ASCII bytes. Authentication
strictly decodes/re-encodes it, computes every readable-key candidate under the
ADR-0011 keyed frame and domain `riffdb.capability-token/v1`, performs every
bounded lookup, and accepts exactly one mutually cross-linked match.

The stable `capabilities` record is keyed by `CapabilityId` and contains the
typed digest reference, nonzero revision, database/environment, stable principal
and actor kind, canonical audiences, issue/expiry time, creation sequence/request
ID, complete `CapabilityGrantV1`, and active or irreversible revoked lifecycle.
The `capability_tokens` lookup is keyed by digest scheme, nonzero `DigestKeyId`,
and 32 digest bytes and contains only the stable ID. Both are versioned
`StoredEnvelope` payloads; raw tokens and key material are never persisted.
Missing, duplicate, or inconsistent cross-links are integrity failures.

`CapabilityGrantV1` has one tenant scope, one all-or-explicit partition scope,
a canonical set of closed permission atoms, canonical entity-field visibility,
`max_scan_rows` in `1..=500`, and the canonical set of permission tags requiring
validated approval. Grammar v1 has no tenant mapping, so commands are authorized
only under global tenant scope unless separately reviewed static metadata exists.
Unknown permission, lifecycle, scope, reason, or obligation tags fail closed.
Command and projection permission requires disclosure of the complete declared
result schema; the POC denies rather than returning a schema-invalid partial
outcome or projection row.

Capability and ADR-0005 idempotency digest keys use distinct typed auth-owned
provider namespaces and exact protected documents. Raw operational key bytes
never enter policy, idempotency, service, runtime, storage records, diagnostics,
or backups. Startup rejects active capabilities whose digest scheme/key is not
readable and rejects reused key material across the two namespaces. Rotation
uses one current write key plus at most seven readable prior keys; key ID
material is immutable by operator contract.

Authentication and authorization use separate synchronous clocks implemented by
server composition. Every service safe point obtains fresh authorization time,
reloads current capability state, and performs a new deny-by-default decision;
positive decisions and clock values are not cached. Capability create/revoke
additionally revalidate transaction-current policy facts after any queue wait
through the narrow pure policy verifier inside their short authoritative
coordinator transaction and before applying the transition.

## 13.3 Authorization decision

Every operation yields a structured decision:

```rust
pub enum Decision {
    Allow { obligations: Vec<Obligation> },
    Deny { code: PolicyCode },
}
```

Obligations are a closed canonical set containing at most one effective tenant
scope, exact partition constraint, field mask, row limit, validated approval
identity, audit class, and output classification. Free-form policy reasons never
cross the public boundary.

`SEC-001` Authorization MUST be deny-by-default.

`SEC-002` A capability MUST be scoped to an environment and MUST NOT be accepted by another environment.

`SEC-003` Revocation MUST take effect on the next request; long-running operations recheck at defined safe points.

`SEC-004` Administrative deployment requires expected active version and an approval reference when policy requires it.

ADR-0050 maintenance authorization is a narrow policy-owned request over the
same current capability facts. It requires global tenant scope, the existing
unparameterized `AdministerCapabilities` permission, current environment and
audience, expiry/revocation checks, and applicable approval obligations. It
adds no durable permission tag. Healthy restore is authorized once against the
current database before drain and again, by fresh authentication and policy
evaluation, against the fully validated staged backup. Empty/corrupt recovery
has no current authorization and requires the staged decision. No prior
principal or decision satisfies the staged check.

Capabilities MUST be bound to durable database identity, configured environment,
and one or more exact configured audiences. Capability creation, revocation, and
catalog activation are authoritative typed administrative operations routed from
the shared service through the commit coordinator and recorded under the separate
ordered nonzero `AdministrationSequence`. No API, service, policy, or
authentication component may write their storage records directly.

Normal capability creation generates the credential server-side and returns it
once only after durable creation. Retrying an equal committed create by the same
caller-selected `CapabilityId` returns typed
`AlreadyCreatedTokenUnavailable`; it does not mint or recover another token or
administration sequence. Reuse of the ID with different normalized content
returns detail-free `CapabilityIdConflict`. Revocation targets the stable ID,
increments its revision exactly once, retains the lookup, and is idempotent.

Capability-revoke authorization facts distinguish a present target from an
absent target. A present target supplies its complete current capability-record
facts: ordinary `RevokeCapability` authority must prove the complete subset
relation, while exact database/environment `AdministerCapabilities` authority
may authorize the administration operation. An absent target supplies only the
checked request ID and target `CapabilityId` plus trusted database and
environment. No target grant, revision, principal, lifecycle, audience, issue
time, or expiry may be invented, so ordinary `RevokeCapability` fails closed;
only current exact-database/environment `AdministerCapabilities` authority may
receive an authorized absent-target preparation.

After a capability queue wait, the coordinator reloads both authorizer and
target, samples the accepted authorization clock, and rechecks mechanically
lowered transaction-current facts. If an authorized absent target remains
absent, storage returns typed `CapabilityNotFound` without a capability-
administration transition sequence; the invocation still has its separate
started and terminal service-audit sequences. If the target has appeared, the
coordinator aborts without a transition or sequence and returns internal
`CapabilityPreparationChanged`. The service reloads the complete now-present
facts, reruns current policy, obtains a fresh bounded executor permit, and
resubmits without appending another `started` record. Changed authorizer facts
deny through the transaction-current verifier and are not reinterpreted as a
preparation change. Capability records are retained, so this absence-to-presence
branch is monotone and does not create an unbounded existence-change loop.

The first local operator uses only the bootstrap mode of
`AdminService.CreateCapability` on the explicitly enabled loopback gRPC listener.
The CLI creates and retains the exact ADR-0009 protected 132-byte bootstrap
credential document; it never supplies the credential through argv and bootstrap
never echoes it. The target actor must be human and the grant must contain
permission `0x13`.

```text
riffdb-bootstrap-credential-v1<LF>
capability-id:<canonical-lowercase-hyphenated-UUIDv7><LF>
token:<43-canonical-token-bytes><LF>
```

The document ends after the third line feed. The CLI reads it only from bounded
stdin or the ADR-0009 protected-file path. For generated output it creates
without overwrite at requested mode `0o600`, writes and synchronizes the file,
closes it, successfully synchronizes the containing directory, and revalidates
the document through the protected-file loader before making any database RPC.
An unsupported or failed file or directory synchronization or protected reread
aborts before transmission. The protected document is retained across an
uncertain response. The public request's capability ID must equal the retained
document ID.

Bootstrap checks transaction-current emptiness of the marker, all capability
records/lookups, active catalog, application commits, and administration stream.
A new bootstrap atomically allocates two consecutive administration sequences:
the principal-less service-audit `started` record first, then the authoritative
capability-administration record together with marker, capability, and digest
lookup. Exact replay appends a new principal-less `started` record linked to the
original transition but creates no second capability transition. The service
must append a separate linked `succeeded` record before releasing either result.
Malformed, mismatched, or failed-emptiness principal-less attempts are bounded
transport-security telemetry and consume no sequence. Unknown compound commit
status fences authoritative writes; recovery retries with the retained
credential and never infers success.

## 13.4 Provenance

Every committed declared command outcome stores:

- Principal and actor kind.
- Optional validated and admitted agent session ID; human/service actors may have none.
- Request ID and idempotency key hash.
- Source repository and commit only when validated and approved by policy.
- Contract version and plan hash.
- Command ID and canonical input hash.
- Partition and conflict-key hashes.
- Outcome type.
- Commit sequence.
- Affected entity identities and versions.
- Durable event identities.
- Approval and reason fields according to policy.

Provenance is immutable. Corrections are additional records linked to the original.

ADR-0012 `ExecutionFailed` retains the exact admitted actor and separately stored
approved provenance claims in terminal admission state, but creates no command
provenance record because no application command committed. An unjournaled
read-only result likewise creates no command provenance. Service-security audit
is separate from command provenance.

## 13.5 Durable service audit

Every structurally valid authenticated command/control-plane mutation and
administrative read is audited through the shared application service. The
storage-owned versioned record uses one coordinator-assigned nonzero
`AdministrationSequence`, request ID, coordinator-observed timestamp, closed operation
and phase tags, optional authenticated principal/capability reference, trusted
ingress, at most 16 safe stable targets, optional validated approval, and an
exact closed linked result: `None`, `Command { commit_sequence, provenance_id }`,
or `ControlPlane { administration_sequence }`. Commit/provenance IDs may also be
independent read targets; the linked result is not a target oneof. The record contains no
credential, digest, idempotency key, business key, cursor, source text, input,
output, free-form reason/error, network address, or transport object.

`riffdb-service` owns the concrete checked pre-sequence `ServiceAuditInput` and
implements the object-safe, `Send + Sync`, commit-owned
`AdministrationAuditInputView`. The view exposes by reference only complete
already-checked semantic fields required by the audit record. It has no field or
method for the new audit record's coordinator-assigned
`AdministrationSequence`, timestamp, storage operation, raw credential,
free-form error, or policy-decision constructor. The view may expose the checked
`ServiceAuditLinkV1` by reference: its
`ControlPlane { administration_sequence }` value identifies an
already-authoritative control-plane transition and is not the new audit record's
assigned sequence. `riffdb-commit` copies that link unchanged, consumes the view,
and never depends on the service crate or duplicates its concrete input. Only the
coordinator/storage transition assigns the new audit record's sequence.
Principal-less bootstrap instead consumes a separate commit-owned opaque,
nonserializable
`BootstrapCompoundAuditProof` constructed only by the checked bootstrap
coordinator path; it is not a general append or authorization capability.

The exact phase tags are started `0x01`, succeeded `0x02`, denied `0x03`,
cancelled `0x04`, failed without an application commit `0x05`, and outcome
uncertain `0x06`; zero and unknown tags reject. `Cancelled` means cancellation
was proven before any authoritative result committed or protected read/stream
establishment became visible; a resumable pending command admission may remain.
Unknown commit status uses outcome uncertain, never cancelled. The exact operation,
ingress, and phase registries are owned by ADR-0007; accepted ADR-0021 owns the
exact target registry and its shared `riffdb-types` newtypes.

The target tags are contract lineage `0x01`, lineage-scoped contract version
`0x02`, entity type `0x03`, command `0x04`, projection `0x05`, index `0x06`,
commit sequence `0x07`, provenance ID `0x08`, and capability ID `0x09`; zero and
unknown tags reject. Every contract numeric ID repeats its lineage. The checked
list contains zero through 16 entries, sorts by the exact ADR-0021 canonical
tag/payload key, and rejects duplicates or a seventeenth entry. Durable decoding
also rejects noncanonical order rather than repairing it. Only
`riffdb-service` derives targets from independently request-addressed objects
known before `started`; authorizing capability, result links, traversed or
returned objects, and business keys/values are never inferred as targets. Start
and terminal records retain the same list.

Capability-administration records and service-audit
records share the one contiguous administration sequence but use disjoint closed
payload/tag registries.

Offline maintenance is the sole additional POC audit exception. Because the
database is closed or replaced, its versioned checksummed external receipt
records the admitted principal, operation/input identity, approval disposition,
phase, and terminal class. WP-155 appends no
`StoredServiceAuditRecordV1` Started or terminal for these three operations and
does not extend the durable 22-operation registry or administration allocator.
The receipt contains no credential, raw path, business value, or free-form
error and cannot authorize another operation.

After successful `RequestContext` construction and exact plan classification,
mutations and administrative reads are intrinsically in audit scope. An allowed
standard read joins only when its policy decision carries an audit obligation;
an ordinary allowed standard read remains unaudited. Every explicit authenticated
policy denial after context appends one standalone `denied`, including a standard
read denial. A standard read's pre-decision semantic/clock failure and an unknown
generic execute target remain bounded redacted telemetry because no audit
obligation or mutating classification exists yet. Malformed or unauthenticated
traffic and rejected principal-less bootstrap attempts likewise remain bounded
transport telemetry. Audit append never audits itself.

An intrinsically audit-required semantic, target, current-policy-clock, or
internal failure after classification but before `started` appends exactly one
standalone `failed`. Neither standalone phase admits work or assigns an
application sequence. An allowed mutation appends durable `started`, waits for a
bounded executor permit, performs the fresh final authorization check, and then
synchronously submits. Post-start denial, clock/internal failure, or cancellation
appends its terminal phase without admission. Every durable `started`, including
new execution, resume, replay, administrative read, and audit-obligated read,
selects exactly one terminal phase and attempts exactly one append when the
service learns the invocation class. At most one terminal record becomes durable;
an append failure, unknown status, or process crash may leave none visible, and
recovery never synthesizes it.

For unary reads and command/control results, the service fully applies current
obligations and constructs a bounded redacted semantic result before appending
`succeeded`; only then may an adapter receive it. A shaping error appends
`failed`, never a false success. Subscription audit covers establishment only:
the service appends `started`, establishes and filters the stream, appends
`succeeded`, then exposes it. Later item-level reauthorization may close the
stream and emits bounded redacted telemetry rather than unbounded durable rows.

A control-plane result uses terminal `succeeded` with
`ControlPlane { administration_sequence }` only when the executor knows the
exact sequence of a new or replayed authoritative transition. Already-active,
already-created, and already-revoked states link that original sequence, while
their API-neutral result shapes remain unchanged; in particular,
`AlreadyCreatedTokenUnavailable` does not expose the sequence. Unknown transition
status uses `outcome uncertain` under the existing recovery rules.

An authorized typed no-transition result instead uses terminal `failed` with
linked result `None`, and the service appends it before releasing the unchanged
typed API-neutral result. This closed POC set is catalog expected-version
mismatch, catalog bundle conflict, capability-ID conflict, and
`CapabilityNotFound`. Internal token-digest collision remains a redacted internal
failure. Bootstrap conflict retains pre-bootstrap bounded telemetry and gains no
fabricated sequence or general audit append.

`riffdb-commit` obtains standalone/start/terminal, catalog, and bootstrap times
from its injected `AdministrationClock`; service/adapters never supply them.
Bootstrap shares one sample across its compound linked records. Normal capability
create/revoke instead uses the one fresh transaction-current `AuthorizationClock`
sample for the verifier, issued/revoked time, and capability-administration audit
so child-expiry validation and durable timestamps cannot diverge. Clock values
need not be monotonic; sequence order is authoritative. Administration-clock
failure is an audit outage: it is not recursively audited, protected work/output
is withheld, and readiness becomes unhealthy.

Before business admission, a required standalone/start append proven abort or
unknown status returns `StorageUnavailable` and submits no business work; unknown
status also fences authoritative writes. A denial remains
`AuthorizationDenied` even when its denial append fails, while still recording a
redacted incident and failing health; an unknown denial append fences writes.
Terminal append failure after a known command commit or durable
`ExecutionFailed` returns `OutcomeUnknown` for same-key resolution. A known
precommit cancellation/failure with no durable terminal identity, and every read
or stream-establishment terminal append failure, returns `StorageUnavailable`
with no protected output. A committed or uncertain control-plane transition uses
its typed unavailable/uncertain recovery result. No audit failure rolls back an
authoritative transition already known durable.

## 13.6 MVP authorization

MVP Streamable HTTP authorization SHOULD conform to the MCP HTTP authorization specification using OAuth 2.1 resource-server behavior. The internal capability model remains authoritative after token validation. OAuth scopes map to capabilities but do not replace tenant, field, command, approval, and environment policy.

---

# 14. Durable effects and outbox

## 14.1 Transaction boundary

A command may emit durable events. It may not send email, call HTTP, charge a card, publish directly to an external broker, or execute user code inside the transaction.

`EFF-001` Event intent and state mutation MUST commit atomically.

`EFF-002` External delivery MUST occur after commit and may be retried.

`EFF-003` Each outbox event MUST carry a stable event ID suitable for downstream deduplication.

P1 proves that event/outbox intent is part of the coordinator's atomic command record set and survives core transaction-path crashes. The external dispatcher, connector retry state machine, and full cross-component crash matrix are P2 work; this staging does not permit a non-durable or post-commit reconstruction of intent in P1.

## 14.2 Outbox state machine

```text
Pending -> Delivering -> Delivered
   |           |
   |           +-> Pending      (retryable failure or worker crash)
   +-> DeadLetter               (non-retryable or attempt policy exhausted)
```

The authoritative command transaction writes one outbox intent for each durable
event and writes no initial `outbox_status` row. Absence of that row is the
canonical never-attempted `Pending` state with zero attempts. A later explicit
pending status may retain bounded retry/backoff metadata. Every status row must
reference exactly one existing intent; an orphan status is an integrity error.
Delivery status remains worker-derived and is never required to prove that the
event intent committed atomically.

Ordinary dispatch may use a rebuilt process-local pending accelerator. Recovery
must instead consume storage's complete bounded undelivered scan. The first
page freezes the greatest visible reciprocal `EventId`; continuation is
exclusive and retains that fence; pages are strict-order, exact-end, at most 500
complete items and 4 MiB encoded content. Each item carries the equal immutable
event/intent plus exact absent-or-present status observation. The scan includes
absent and explicit `Pending`, `Delivering`, and `DeadLetter`, excludes
`Delivered`, and rejects orphan/mismatched rows. Only WP-160 interprets retry
policy or normalizes `Delivering`.

A connector declares:

- Destination type and configuration reference.
- Delivery timeout.
- Retry schedule.
- Maximum attempts.
- Whether the destination accepts idempotency keys.
- Response classification rules.
- Redaction and audit policy.

## 14.3 POC connector scope

The POC MUST provide:

- A `stdout` or file-test connector for deterministic demonstrations.
- A fake connector used by crash and duplicate-delivery tests.
- An optional HTTP webhook connector disabled by default and clearly outside command execution.

The POC promises atomic event intent and at-least-once dispatch. It does not promise exactly-once external effects. Effective-once business behavior requires downstream idempotency or reconciliation.

## 14.4 Delivery record

```rust
pub struct DeliveryState {
    pub event_id: EventId,
    pub state: DeliveryStatus,
    pub attempts: u32,
    pub last_attempt_at: Option<Timestamp>,
    pub next_attempt_at: Option<Timestamp>,
    pub destination_id: String,
    pub last_safe_error: Option<String>,
}
```

Connector responses and errors MUST be size-limited and scrubbed before persistence.

---

# 15. Projection plane for the POC

## 15.1 Scope

The POC projection engine exists to prove transaction-to-projection consistency, not to prove a complete OLAP engine.

Supported operators:

- Event filter.
- Key projection.
- Count.
- Sum.
- Grouped count and sum.
- Deterministic derived scalar expressions over aggregate values.

Unsupported operators:

- Joins.
- Exact distinct count.
- Windows.
- User-defined functions.
- Arbitrary retractions from mutable source tables.
- Approximation.

## 15.2 Consumption and frontier

Projection durable identity is the exact tuple of contract lineage, nonzero
`ProjectionId`, and `ProjectionPlanHash`. It excludes application contract
version and bundle hash so an unchanged plan may retain state across an unrelated
compatible deployment; a changed plan hash creates a disjoint identity and
rebuilds from `BeforeFirst`.

Each identity has nonzero, never-reused `ProjectionGeneration` values. The
control record owns the highest allocated generation, optional published and
candidate generations with their `FrontierPosition`, published apply mode,
closed lifecycle, and optional closed failure. `FrontierPosition` is exactly
`BeforeFirst` or `AppliedThrough(nonzero CommitSequence)`; semantic code never
constructs sequence zero as an empty sentinel.

The projection worker scans every authoritative commit in strictly increasing
sequence, including commits with no relevant event. For each retained generation
it writes exactly one apply marker per applied sequence. State row post-images,
the marker containing the canonical domain-separated `ProjectionApplyHash`, and
the matching frontier advance are one atomic transaction. A duplicate is an
equal no-op only when identity, generation, sequence, and apply hash all match;
gaps, missing markers, hash mismatch, or frontier inversion fail closed.

Before filter, key, or measure evaluation, the worker MUST pass each relevant
immutable event, together with its enclosing commit's exact writer-plan
reference, through the catalog-owned event-materialization view for the exact
resolved projection bundle. Live catch-up and rebuild use this identical path.
The worker MUST NOT evaluate a raw durable payload as though it already matched
the projection's source-event schema, and it MUST NOT persist the normalized
view or replace the event's original bytes or hash.

The catalog result is fields-private, process-local, and nonserializable. It
binds the exact resolved projection bundle/plan, enclosing commit's complete
writer `ExecutablePlanRef`, and immutable event; proves strict-ancestor
optional-null authority; preserves but hides unknown fields; and exposes only
schema-directed checked access. Foreign, descendant, malformed same-version,
genesis, required omission, wrong-type, hard-limit, or integrity cases fail
closed. Projection uses the shared pure invariant evaluator only for the
checked filter/group/measure expressions over that view. It receives no raw
payload, omission mask, ancestry proof, command evaluator, or storage-
implementation type.

Group state keys use versioned prefix `0x47 0x01`, the exact projection identity,
generation, and length-framed ADR-0011 canonical scalar group values. Apply keys
use `0x41 0x01`; the generation-neutral control key uses `0x46 0x01`. Grouping
supports the exact ADR-0017 closed scalar registry, including decimal and money,
and only complete-key equality or complete leading-component prefix scans.
Keys, rows, apply requests, query pages, and snapshots obey ADR-0017's fixed
bounds; raw group keys/values are redacted by default.

One projection has 1..=1,024 group components. A state semantic payload is at
most 1 MiB; one apply request has at most 4,096 row updates and 15 MiB canonical
semantic content; its complete rows/marker/control write set and one apply
snapshot are at most 16 MiB; one query returns at most 500 rows and 4 MiB row
content. Compilation rejects a group schema whose complete key or stored row
maximum can exceed its bound.

The closed lifecycle is `Building`, `CatchingUp`, `Ready`, `Rebuilding`,
`Degraded`, or `Invalid`, with exact published/candidate/apply-mode/failure
shapes from ADR-0017. Absence of control for an identity known to the checked
bundle is normal uninitialized state: public status/query maps it to building at
`BeforeFirst`, not ready or corrupt. Unknown identities remain not-found.

`PRJ-001` The published frontier of one exact `ProjectionIdentity` MUST never
decrease. Replacing a generation for the same identity cannot lower it; a changed
plan hash creates a new identity whose independent initial position is
`BeforeFirst`.

`PRJ-002` A frontier MUST never advance past a commit whose relevant events have not been durably applied.

`PRJ-003` Projection application MUST be idempotent with equality validation by
`(ProjectionIdentity, ProjectionGeneration, CommitSequence)`; a rebuild
generation intentionally reapplies the authoritative prefix in a disjoint
derived-state namespace. Within that identity, duplicate equality MUST also
require exact `ProjectionApplyHash` equality; a mismatch fails closed.

`PRJ-004` Projection state MUST be rebuildable from the authoritative commit log.

Initial build creates generation 1 as an unpublished candidate and applies the
log from sequence 1. Publication is one control transition only when its
frontier equals the transaction-current authoritative head. A same-plan rebuild
allocates the next generation, retains the published generation, applies into a
disjoint namespace, and publishes only when the candidate is not behind the old
published frontier and equals the current head. Candidate rows are never public.
Replaced generations, including failed candidates once an explicit recovery
transition replaces them, are retired: their canonical rows and markers may
remain, but they are never queried, resumed, reused, or accepted as apply targets
and have no active frontier. A failed candidate still retained by `Degraded`
control is suspended and inert but is not retired until replacement. Garbage
collection is deferred to a whole-generation crash-safe policy.

A deterministic overflow, malformed event, missing commit/plan, schema mismatch,
integrity failure, or hard-limit failure records one closed generation failure
without advancing. A failed published generation is suspended until replacement
publication. `Invalid` has no v1 recovery transition; authoritative commits or
derived rows are never fabricated. Generation exhaustion never wraps and leaves
the prior control record unchanged.

## 15.3 Read-after-commit query

A query accepts an optional real nonzero `after_sequence`, bounded wait deadline,
schema-checked generation-neutral complete/prefix selector, bounded limit, and
opaque service cursor. Storage reads lifecycle/control, selects the published
generation, scans rows, and returns rows plus frontier and lower continuation
from one read transaction. It never joins a control read to a later scan.

```rust
pub enum ProjectionQueryResult<T> {
    Ready {
        data: T,
        frontier: FrontierPosition,
    },
    WaitTimedOut {
        required: CommitSequence,
        current: FrontierPosition,
    },
    Degraded {
        current: FrontierPosition,
        reason: ProjectionUnavailableReason,
    },
    Invalid {
        reason: ProjectionUnavailableReason,
    },
}
```

The lower storage result additionally carries one internal atomic state fence:
degraded has exact identity, optional retained generation, and current
frontier; invalid has exact identity, failed generation, and current frontier.
Generation is absent only for a known control-less identity at `BeforeFirst`
with reason `Building`. The service validates this internal fence and does not
issue a second status read; the public result above remains unchanged.

`ProjectionUnavailableReason` is closed: `Building`, `Rebuilding`, or
`Failure(ProjectionFailureCode)`. Stored/engine strings are never public. A known
identity without control and `Building`/`CatchingUp` return degraded-building
with no rows; `Rebuilding` returns degraded-rebuilding with no rows even while a
published generation is retained; durable `Degraded` and `Invalid` expose only
the closed safe failure. `Ready` behind the requested sequence waits and returns
`WaitTimedOut` on deadline. An unavailable lifecycle never becomes `Ready`
merely because `after_sequence` is absent.

The server MUST NOT silently return stale data when `after_sequence` is provided
and the frontier is behind. Every wake rechecks current policy and rereads one
complete persisted snapshot. A pagination continuation fences exact identity,
published generation, prefix, last key, and observed published frontier; any
generation or frontier change invalidates it and returns no rows. The service
maps that to its generic policy-safe invalid-cursor result rather than silently
restarting or misreporting lifecycle.

Projection status returns the exact identity, lifecycle, optional
published/candidate generation and `FrontierPosition`, optional published apply
mode and closed failure, and transaction-current authoritative head from one
read transaction. Lag is derived, not persisted. WP-065 owns the durable
projection messages and checked storage mappings; WP-127 owns the public
query/status/frontier/lifecycle/page messages; WP-130 owns total wire conversion.

## 15.4 Budget projection

`BudgetUtilizationDaily` consumes `BudgetAllocated` events and groups by organization, fiscal year, and transaction date. The acceptance demo queries the projection using the winning allocation's commit sequence and verifies the result includes exactly one 80-unit allocation.

---
# 16. Operations, configuration, and observability

## 16.1 Processes and binaries

| Binary | POC responsibility |
|---|---|
| `riffdbd` | Database server, gRPC API, optional loopback MCP HTTP endpoint, commit coordinator, projection worker, outbox worker, health and metrics. |
| `riffdb` | Contract validation and deployment, capability administration, command invocation, entity and commit inspection, projection queries, backup and restore, demo orchestration. |
| `riffdb-mcp` | Stdio MCP bridge that forwards one checked bearer presentation through the public gRPC client; `riffdbd` performs authentication. It contains no local authentication, storage, or command semantics. |

The POC ships exactly these three binaries. WP-135's nested-workspace
`riffdb-budget-public` and WP-139's nested-workspace `riffdb-budget-safety` are
long-lived comparison/evidence runners and are not shipped RiffDB product
binaries. Deterministic code generation, when needed, is a `riffdb contract
generate` subcommand. Replay, durable inspection, and recovery helpers remain
test-harness code in the POC; a privileged offline repair binary is deferred to
Stage A and requires a separate authorization and audit design.

WP-150 implements exactly the 12 public command identities
`contract.validate`, `contract.deploy`, `command.execute`, `command.outcome`,
`entity.get`, `commit.show`, `projection.query`, `capability.bootstrap`,
`capability.create`, `capability.revoke`, `server.health`, and `demo.budget`.
Every database operation uses `riffdb-client-rust` against the configured
loopback gRPC endpoint. CLI-local work is limited to bounded argument/config/
input parsing, checked rendering, bootstrap generation, crash-safe credential
retention, and direct launch of the accepted public budget runner. It never
opens redb, instantiates a service, authenticates a principal, evaluates policy,
compiles/executes a command internally, or constructs a semantic result.

Backup and restore remain mandatory for POC exit and are not WP-150 commands.
Accepted ADR-0050 activates WP-155 after WP-185. It adds exactly
`CreateOfflineBackup`, `RestoreOfflineBackup`, and
`GetOfflineMaintenanceOperation` to the public AdminService and
`backup.create`, `backup.restore`, and `backup.operation` to the public-only
CLI. They use the API-neutral service, current/staged policy, the configured
backup root, and WP-070's existing artifact format. They are not MCP
operations, hidden file copies, redb bypasses, or additions to the durable
22-value service-audit registry.

## 16.2 Configuration precedence

Server configuration precedence, and any setting without a narrower accepted
interface, is highest first:

1. Explicit CLI flags.
2. Environment variables prefixed `RIFFDB_`.
3. A TOML configuration file.
4. Built-in safe defaults.

Secrets MUST NOT be accepted from the TOML file unless the file mode and deployment environment satisfy a documented local-development policy. Production-grade secret delivery is an MVP concern.

The WP-150 client configuration instead has exactly four fields and this per-
field precedence:

| Field | Flag | Environment | TOML | Default |
|---|---|---|---|---|
| Endpoint | `--endpoint` | `RIFFDB_ENDPOINT` | `client.endpoint` | `http://127.0.0.1:7443` |
| Output | `--output` | `RIFFDB_OUTPUT` | `client.output` | `human` |
| Attempts | `--max-attempts` | `RIFFDB_MAX_ATTEMPTS` | `client.max_attempts` | `3` |
| Credential file | `--credential-file` | `RIFFDB_CREDENTIAL_FILE` | `client.credential_file` | absent |

A present empty/invalid higher-precedence value rejects rather than falling
through. Output is exactly `human | json`; attempts are canonical decimal
`1..=10`. TOML is read only from `--config`, then `RIFFDB_CONFIG`, then absent;
there is no directory discovery. Its only table is optional `[client]` with only
the four optional keys above; input is complete UTF-8 at most 65,536 bytes and
unknown/duplicate/wrong-type items reject. Raw credentials/bootstrap documents
are forbidden. Only `RIFFDB_CONFIG`, `RIFFDB_ENDPOINT`, `RIFFDB_OUTPUT`,
`RIFFDB_MAX_ATTEMPTS`, `RIFFDB_CREDENTIAL_FILE`, and
`RIFFDB_CAPABILITY_TOKEN` are read; other `RIFFDB_*` variables are ignored.

All CLI paths are nonempty, NUL-free, and at most 4,096 platform bytes; argv
paths need not be UTF-8. The endpoint is at most 512 ASCII bytes and exactly
lowercase `http://<loopback-IP-literal>:<port-1..65535>` with no DNS, user info,
path, query, fragment, whitespace, implicit port, TLS, or normalization. Contract
source, CLI JSON, and general stdin are streamed with a 1,048,576-byte limit and
one-byte excess probe; configuration and the exact 132-byte bootstrap document
retain tighter bounds. The checked terminal model and fully staged rendering
are each at most 4,194,304 bytes; any oversize/rendering failure leaves stdout
empty.

Representative configuration:

```toml
[server]
data_dir = "./data"
grpc_listen = "127.0.0.1:7443"
mcp_listen = "127.0.0.1:7444"
mcp_origins = ["http://127.0.0.1:3000"]
max_request_bytes = 1048576
shutdown_grace_ms = 10000

[storage]
engine = "redb"
group_commit_max_delay_ms = 2

[transactions]
lock_wait_ms = 5000
nondeterministic_test_scheduler = false

[projection]
enabled = true
poll_interval_ms = 25
max_batch_commits = 1000

[outbox]
enabled = true
max_in_flight = 32

[observability]
log_format = "json"
log_level = "info"

[maintenance]
backup_root = "/var/lib/riffdb/backups"
```

Configuration parsing MUST reject unknown keys by default. Every setting MUST document whether changing it requires restart and whether it affects correctness, compatibility, security, or only performance.

Hosted MCP is disabled when no `mcp_listen`/`--mcp-listen` value is present.
The value is a literal loopback address with a nonzero port. Each configured or
repeatable `--mcp-origin` value uses WP-140's exact bounded loopback-origin
grammar; an empty list permits only requests without `Origin`. The protected
resource audience is derived exactly as
`http://<canonical-listen-authority>/mcp`, never configured independently.
The POC metrics registry is in-process; `metrics_listen` is not an accepted
setting and no network exporter is started. `backup_root` is an absolute
server-owned path, and public requests select only a checked `BackupNameV1`
beneath it.

## 16.3 Health model

`riffdbd` exposes:

- **Liveness**: process event loop is functioning.
- **Readiness**: storage opened, active contract loaded, commit coordinator accepting work, and required migration checks complete.
- **Degraded state**: core writes remain available but a non-authoritative subsystem such as projection or outbox is unhealthy.

`Initializing` is distinct from corruption and readiness. A structurally empty
database without an active contract reports not ready. During initializing
validation and, after successful validation, while the bootstrap marker remains
absent, the unchanged Health operation may use the API-neutral principal-less
`PreBootstrapHealthContext`. `riffdb-service` owns that privately constructible,
nonserializable context; the server lifecycle router may construct it only for
those two pre-marker phases. It confers no principal, credential, authorization
or policy fact, storage handle, audit authority, or access to another service
operation.

The pre-bootstrap result contains exactly a closed bounded lifecycle value,
liveness, and readiness. It MUST NOT expose an active contract or version,
commit position, component inventory or diagnostic, storage handle or identity,
policy fact, build information, process start time, or any extensible text or
map. It emits no durable service audit. The same public Health RPC maps this
restricted result; there is no additional RPC or privileged storage path.

The service and server MUST stop admitting new pre-bootstrap health contexts
once the bootstrap marker is durably committed. In `DeploymentRequired`, at
full readiness, and on every later lifecycle path, Health requires the ordinary
authenticated `RequestContext`, current policy evaluation, and applicable
obligations/redaction before release. One-time loopback bootstrap while the
marker is absent remains the sole principal-less mutation and the sole operation
that may create durable service-audit records without a principal. Authenticated
first contract deployment after bootstrap still uses the shared service,
authorization, audit, and coordinator. Once an active contract exists, its later
absence or mismatch is a fail-closed authoritative integrity error.

WP-130 owns this minimum production lifecycle and the first runnable `riffdbd`:
identity initialization through the commit executor, complete redb structural
evidence, matching catalog historical validation, readable capability and
idempotency digest inventories, bootstrap, deployment, active readiness, gRPC,
and clean restart. WP-185 does not replace that gate. It extends the same
component graph with MCP HTTP, outbox/projection workers, observability, and
separate derived-health aggregation; a derived failure cannot retroactively
invalidate the accepted authoritative startup proof.

ADR-0050 adds a private maintenance transition
`Ready -> Draining -> Offline -> Validating -> Ready | FailedClosed`.
Draining stops new ordinary and MCP admission and proves bounded quiescence
before redb closes. No protected health, operation polling, or other request is
served while offline. An empty/corrupt startup exposes only restricted health
and the exact staged-backup restore admission; it never fabricates current
readiness or authorization. Backup/restore reaches Ready only after the normal
complete post-operation startup proof and a durable terminal external receipt.

Construction begins with one Health-only `InitializingRiffDbService`, one
move-only activation authority, and one issuer sharing the same pre-bootstrap
admission. The initializing value owns no executor, storage, catalog, policy,
audit, token, projection, outbox, cursor, or operational capability. The
listener routes restricted Health through that API-neutral value while startup
proofs run and atomically replaces it with the complete `RiffDbService` only
after matching structural and catalog history validation. No transport may
construct either Health result directly or activate dormant capabilities.

```rust
pub enum HealthResult {
    PreBootstrap(PreBootstrapHealthReport),
    Authenticated(HealthReport),
}

pub struct PreBootstrapHealthReport {
    pub lifecycle: PreBootstrapLifecycle,
    pub liveness: bool,
    pub readiness: bool,
}

pub struct HealthReport {
    pub status: HealthStatus,
    pub active_contract_version: Option<ContractVersion>,
    pub last_commit_sequence: Option<CommitSequence>,
    pub components: Vec<ComponentHealth>,
    pub started_at: Timestamp,
    pub build: BuildInfo,
}
```

A projection or outbox failure MUST NOT falsely mark committed source state as rolled back. Health output MUST distinguish authoritative transaction-path failures from derived-system failures.

## 16.4 Structured tracing

The implementation MUST use `tracing` spans across all API entry points. Every command path includes, where available:

```text
request_id
transport
principal_id_hash
agent_session_id_hash
command_id
contract_version
plan_hash
partition_key_hash
conflict_key_count
lock_wait_ms
read_dependency_count
mutation_count
outbox_event_count
commit_sequence
storage_queue_ms
commit_ms
outcome_type
replayed
```

Raw business identifiers, secrets, free-form input values, and event payloads MUST NOT be recorded by default. A redaction layer MUST operate before data reaches subscribers.

Redaction at a subscriber layer alone is insufficient. Service, commit,
conflict, storage, MCP, catalog/auth/policy, outbox, and projection owners emit
only their closed ADR-0049 event vocabularies through injected least-authority
sinks. Those values contain only stable closed tags, checked numeric quantities,
and the expressly permitted pre-redacted hashes above; they accept no arbitrary
map, label, message, diagnostic, business value, key, credential, or payload.
Only after that validation may observability create a tracing event. A
permissive sibling subscriber must still be unable to observe redaction
canaries.

## 16.5 Metrics

Minimum POC metrics:

- Command request count by command, transport, and terminal class.
- Command latency histogram and storage-queue latency histogram.
- Lock wait histogram, timeout count, queue depth, and hot conflict-key cardinality using bounded labels.
- Commit count, commit duration, commit batch size, and durable flush duration.
- Idempotency hit, mismatch, and uncertainty-recovery counts.
- Active contract version and deployment count.
- Outbox pending count, attempt count, age of oldest pending event, and dead-letter count.
- Projection frontier, lag in commits, rebuild count, and error count.
- MCP session count, tool calls by risk class, schema failures, authorization denials, and list-change notifications.
- Storage bytes, commit-log records, and startup recovery duration.

Metrics MUST NOT use unbounded tenant, request, entity ID, conflict key, or error message labels.

The inventory, label registries, counters, current gauges, histograms, capacity
limits, and dropped-observation counters are frozen and tested in
`riffdb-observability`. Semantic event types remain owned by their source
crates, which do not depend on observability. WP-185 wires those sinks to one
in-process registry. The POC exposes no network metrics endpoint.

## 16.6 Administrative CLI examples

```bash
riffdb contract validate contracts/budget.riff
riffdb contract deploy contracts/budget.riff --expected-version 0
riffdb command execute AllocateBudget --input request.json
riffdb command outcome AllocateBudget --idempotency-key 81e6...
riffdb entity get Budget --key organization_id=... --key fiscal_year=2026
riffdb commit show 42
riffdb projection query BudgetUtilizationDaily --after 42 --input query.json
riffdb server health
riffdb backup create before-upgrade
riffdb backup operation 01900000-0000-7000-8000-000000000000
riffdb backup restore before-upgrade --confirm-replace-current-database
```

CLI JSON output MUST be stable enough for integration scripts. Human-formatted output is additive and MUST not replace a machine-readable mode.

Machine mode is `--output json` and emits exactly one compact UTF-8 JSON object
plus LF, with no other stdout bytes. Its closed success envelope and key order
are `schema`, `command`, `ok`, `result`; the error envelope replaces only the
last key with `error`. `schema` is exactly `riffdb.cli.output/v1`, `command` is
the exact lowercase dotted command identity, `ok` is Boolean, and exactly one
of result/error exists. Each command owns a closed DTO and never serializes generated
Protobuf, unrestricted maps, arbitrary server text, or Rust `Debug`. Human
output is explicitly unstable. WP-150 must first submit the complete clap
grammar, configuration examples, exit-code registry, closed result/error DTOs,
and all command/terminal-branch JSONL goldens as an interface-only PR for
separate human acceptance; ADR-0041 accepts the common envelope/scalar rules,
not those not-yet-authored exact command fixture bytes.

In CLI JSON, every `u64`/`i64` uses a canonical decimal string, smaller integers
use JSON numbers, UUIDs are lowercase hyphenated, typed hashes are fixed
lowercase hex, and other bytes use padded standard base64. Optional absence
omits a key; explicit RiffDB null remains tagged; enums are checked lowercase
snake case; object/repeated ordering follows the closed DTO and semantic order.
RiffDB Values use the exact ADR-0041 closed `type`-tagged forms for null, bool,
i64, u64, decimal, money, string, bytes, uuid, date, timestamp, enum, list, and
record. Record input fields have a nonzero ID, bounded source name, or both, and
the shared service resolves/equates them. No untagged native JSON, float,
alternate byte spelling, unknown property, truncation, or generic recursive
conversion is allowed.

A normal credential comes from exactly one of
`RIFFDB_CAPABILITY_TOKEN` or the resolved protected credential file. There is
no raw-token flag, positional argument, TOML value, stdout/stderr echo, or shell
construction. File input uses only the public-client loader. The environment
value is checked as one exact 43-byte presentation, promptly moved into a
zeroizing buffer and redacted `BearerCredential`, and never decoded locally.

Bootstrap generation/input uses only isolated
`riffdb_auth::bootstrap_secret`, bounded stdin or protected file, and the exact
132-byte document. `capability.bootstrap` may add `--bearer-output`; it writes
the same retained 43-byte token presentation, never derives another. Generation
mode exclusively creates, file-syncs, closes, directory-syncs, and protected-
rereads both requested outputs before RPC. Existing-input mode never rewrites
its source and applies that process only to an optional bearer output. Normal
capability create likewise requires an exclusive protected output, completes
file/directory durability and a public-loader presentation comparison before
reporting success, and never prints the one-time token. Failure leaves retained
files for explicit operator handling, performs no delete/overwrite, and claims
no recovery from a lost create response.

CLI retries use only the three WP-137 helpers with explicit `max_attempts`
including the first call. Every attempt has a fresh outer RequestId while
command/idempotency input, bootstrap document/ID/token, or normal-create
CapabilityId stays identical. It never retries definitive failures, invents a
new key/capability, refreshes expected versions/cursors/query input, parses Tonic
status, or treats a local file as proof of server success.

The comparison command is exactly
`riffdb demo budget --runner <path> --case
sequential|contention|same_key_replay`. It requires a resolved credential-file
path and directly launches the upstream `riffdb-budget-public` protocol from
WP-135 with no shell, empty environment, closed stdin, independent 4,096-byte
stdout/stderr caps, and a fixed 180-second kill-and-reap deadline. Only the
closed one-line/exit combinations in ADR-0041 are accepted; child output is not
relayed. The runner itself accepts exactly the eight ordered arguments for
protocol, case, endpoint, and protected credential path, emits the exact
`riffdb.budget.public-run/v1` success line or fixed failure stderr/exit, and uses
only public gRPC plus the shared workload oracle.

```text
riffdb-budget-public --protocol riffdb.budget.public-run/v1 \
  --case sequential|contention|same_key_replay \
  --endpoint http://<literal-loopback-ip>:<nonzero-port> \
  --credential-file <protected-43-byte-file>
```

Success writes exactly one compact line with ordered keys `schema`, `adapter`,
`case`, `workload_version`, `status`, where the fixed values are respectively
`riffdb.budget.public-run/v1`, `riffdb-public-grpc-v1`, selected case, numeric
`1`, and `passed`, then exits 0. Checked API/oracle/preflight failure writes no
stdout, exact stderr `riffdb budget public run failed\n`, and exits 1. Invalid
invocation/configuration writes no stdout, exact stderr
`riffdb budget public invocation invalid\n`, and exits 2. Any other exit/signal
or output combination is invalid runner behavior.

---

# 17. Correctness, testing, and verification

## 17.1 Testing strategy

Correctness work is part of implementation, not a final hardening phase. Each semantic subsystem requires:

1. Unit tests for local rules.
2. Property tests over generated inputs and histories.
3. Model-based tests against a deliberately simple reference model.
4. Deterministic or explored concurrency schedules.
5. Process-level crash and restart tests where durability is involved.
6. API conformance tests for gRPC and MCP surfaces.
7. End-to-end tests that exercise every shared service path.

## 17.2 Reference model

`riffdb-testkit` MUST include an in-memory, single-threaded reference model with no optimized storage or concurrency. It interprets the same compiled command semantics and produces:

- Entity state.
- Persisted outcomes by idempotency identity.
- Ordered commit records.
- Durable events.
- Projection state.

Generated command histories run against the reference model and the real server. After each terminal result, observable state and outcome semantics MUST match modulo explicitly documented implementation metadata.

## 17.3 Property suites

| Property | Scope |
|---|---|
| Invariant preservation | Every committed entity set satisfies every supported invariant. |
| Outcome determinism | Same owned snapshot, normalized input, exact plan, transaction context, and fixed evaluation budget produce the same `ExecutionResult`, `EvaluatedCommand`, or typed fault; the POC command runtime has no randomness. Coordinator tests separately freeze final `CommitIntent` assembly from that value and the exact stored admission. |
| Idempotent replay | Repeating an admitted idempotency identity with equal canonical input never creates a second mutation or event set. |
| Idempotency mismatch rejection | Reusing an identity with a different canonical input never returns the earlier outcome as though it applied. |
| Conflict serialization | Histories sharing a mutable conflict key are observationally equivalent to an allowed serial order. |
| Lock release | Cancellation, business rejection, panic containment, and storage failure release all acquired capabilities. |
| Sequence monotonicity | Every committed declared outcome has one unique increasing nonzero application sequence; pending and ADR-0012 `ExecutionFailed` admission records have none. Administration records use a separate contiguous nonzero sequence space. |
| Recovery equivalence | Reopening after any completed durable prefix yields the same observable state as replaying that prefix in the model. |
| Projection prefix | A published projection identity/generation at `AppliedThrough(N)` equals model application of every authoritative sequence through N, including irrelevant-event markers; `BeforeFirst` has no marker or visible row. |
| Authorization non-bypass | No transport reaches command, entity, commit, or deployment operations without a policy decision. |

## 17.4 Concurrency testing

Use three layers:

- **Loom** for lock-table state transitions, waiter registration, cancellation races, and coordinator notification primitives.
- **Shuttle** for larger runtime schedules involving command tasks, storage coordinator, projection worker, and outbox worker.
- **Process integration tests** for actual Tokio, redb, gRPC, and MCP behavior.

The lock manager MUST expose a test-only deterministic scheduler abstraction. Tests MUST include:

- Two writers for one key.
- Multiple-key acquisition in opposite user order but canonical runtime order.
- Waiter cancellation before grant.
- Cancellation concurrent with grant.
- Lock holder panic.
- Storage failure after runtime evaluation.
- Read-only command concurrent with mutation.
- A dynamically discovered undeclared dependency causing a safe rejection.
- Arithmetic/resource fault with concurrent change to every present, absent, and
  range-epoch dependency; stale evidence never terminalizes `ExecutionFailed`.

## 17.5 Crash and failpoint matrix

The POC MUST support named, test-only failpoints. Process tests terminate the server rather than relying only on recoverable error returns.

| Failpoint | Required recovered state |
|---|---|
| Before command admission | No outcome, mutation, event, or sequence. |
| After pending idempotency reservation, before locking | Only the valid pending reservation is durable; retry reuses its contract version, plan, and `tx.time`; no sequence exists. |
| After lock acquisition, before evaluation | Pending reservation may remain; locks are absent after restart; no terminal state or sequence exists. |
| After evaluation, before commit submission | Pending reservation may remain; no mutation, event, outcome, or sequence is visible. |
| During execution-failure terminalization | Either the equal-evidence `ExecutionFailed` admission is durable with no application sequence/provenance/event/commit, or the original pending admission remains; lineage-expansion `ResourceLimit` additionally requires exact raw-observation equality and deterministic reproduction of the same overflow; unknown status fences writes and same-key recovery resolves it. |
| Before storage transaction commit | No partial mutation, outcome, event, or sequence. |
| After capacity reservation, before sequence assignment | No sequence or record graph is assigned or staged; rollback releases the reservation. |
| After sequence assignment, before canonical-envelope verification/staging | The assignment remains transaction-local and invisible; a verification failure or crash exposes no sequence or partial graph. |
| During embedded-engine durable commit | Either entire atomic commit is visible or none, according to engine guarantee. |
| After durable commit, before coordinator response | Complete state visible; retry returns persisted result. |
| After coordinator response, before API response | Complete state visible; retry returns persisted result. |
| After event/outbox-intent commit, before dispatch | Event and matching intent are durable; no status row canonically means never-attempted `Pending`, and dispatch is eventually attempted. |
| During outbox delivery-status transition | Status is absent/previous or one complete state; an orphan status fails integrity, and restart idempotently normalizes `Delivering` according to retry policy before worker readiness. |
| After destination success, before delivery acknowledgment | Duplicate dispatch is allowed; connector/idempotency policy handles it. |
| During projection row/marker/frontier commit | State post-images, exact apply-hash marker, and matching generation frontier are all visible or none; replay is an equal-hash no-op and a mismatch fails closed. |
| After frontier commit, before query wakeup | Query observes persisted frontier after restart. |
| During contract deployment before active pointer switch | Previous bundle remains active. |
| After active pointer switch, before API acknowledgment | New bundle is active and deployment retry is resolved by expected version. |
| During new-database metadata initialization | Either the complete initial metadata including one `DatabaseId` is durable or the store remains truly uninitialized; reopen never substitutes a new identity. |
| After provenance-ID generation, before/during command commit | A proven abort exposes no candidate or sequence; a collision writes nothing; unknown commit status is resolved by idempotency before another candidate is requested. |
| During maintenance receipt transition | The prior complete checksummed receipt or the next complete monotonic receipt is authoritative; bad checksum, impossible phase, or uncertain parent sync fails closed. |
| After backup artifact publication, before terminal response | The immutable named artifact is complete and matches the receipt input, or is absent; same-operation recovery validates it and never publishes a second artifact. |
| During staged restore or staged authorization | The current target is unchanged; no staged principal/decision is reused and no credential is persisted. |
| After destructive target publication, before validation/receipt | The exact restored artifact is present but readiness remains false; recovery repeats full validation and reconciles the receipt before any protected request. |

`TEST-001` Every failpoint MUST have an automated assertion over entities, indexes, outcomes, commit records, outbox records, and projection frontier as applicable.

The storage/coordinator matrix additionally asserts that an equal per-class
canonical-envelope bound stages, a one-byte excess aborts before staging without
consuming a sequence, and every recovered durable event recomputes to its stored
ADR-0011 `EventHash`.

## 17.6 Parser, protocol, and storage fuzzing

`cargo-fuzz` targets:

- Contract lexer and parser.
- Contract source-to-AST round-trip where supported.
- Protobuf record decoder and envelope validation.
- Canonical key and value encoders.
- JSON-to-contract-type conversion.
- MCP tool input validation and structured result conversion.
- Snapshot, backup manifest, and external maintenance receipt parsers.

Fuzz targets MUST enforce input size and recursion limits matching production behavior.

## 17.7 Snapshot testing

Use `insta` for reviewable snapshots of:

- Compiler diagnostics with source spans.
- `EXPLAIN COMMAND` output.
- Generated JSON Schemas.
- Generated MCP tool definitions.
- Rust SDK signatures.
- Contract compatibility reports.
- Redacted provenance and commit rendering.

Snapshots MUST not substitute for semantic assertions.

## 17.8 Benchmarks

Use dependency-free repeated-run component harnesses and a separate process
harness for end-to-end results. WP-070 MUST NOT add Criterion merely to compile
the storage benchmark; introducing any benchmark framework later requires the
normal dependency review. Every benchmark records:

- Git revision and dirty state.
- Rust version and target.
- Build profile and enabled features.
- CPU, memory, storage medium, operating system, and filesystem.
- Database size, contract version, workload distribution, and durability mode.
- Confidence interval or repeated-run distribution.

Benchmark scripts MUST be checked into the repository. Results used for an architecture decision MUST be attached to the relevant ADR.

The repository maintains one long-lived budget comparison application beginning immediately after the budget contract and typed IR stabilize. A transport-neutral workload specification, deterministic seed data, operation mix, result oracle, and guarantee profile are shared by:

- A PostgreSQL implementation in a nested non-production workspace, using explicit SQL transactions and constraints appropriate to PostgreSQL.
- A RiffDB implementation that first uses the API-neutral application service, then adopts the canonical Rust SDK/gRPC path when it exists.

Correctness preflight MUST pass before comparative performance results are reported. Reports MUST distinguish matched semantics from weaker or stronger guarantee profiles, including isolation, durability, idempotency, outbox, and projection behavior. PostgreSQL client libraries, SQL, and migration tooling MUST NOT become dependencies of RiffDB production crates. WP-045 establishes the workload, oracle, and PostgreSQL baseline; WP-125 adds the service adapter; WP-135 makes SDK/gRPC canonical; WP-150 packages the demo; and WP-200 reuses the same runner for reproducible comparison evidence.

WP-139 adds a separate correctness-only safety report after WP-135. It preserves
the canonical PostgreSQL adapter and frozen `riffdb.budget.public-run/v1`
runner, never benchmarks deliberately unsafe variants, and qualifies every
claim: PostgreSQL supports safe implementations, while the counterexamples show
that the corresponding hazard is absent or rejected through RiffDB's supported
application mutation and compiled-contract surfaces. Its exact four scenarios
are unlocked lost update, direct-DML precondition bypass, duplicate allocation
after a deliberately discarded response, and same-key/different-input reuse.
Real TCP-loss/restart evidence remains WP-190/WP-200.

The Fjall comparison is likewise isolated in a nested non-production workspace with its own lockfile. WP-075 runs the unchanged storage semantic conformance suite against that adapter. WP-200 publishes the resulting correctness, recovery, and performance evidence for the POC-exit engine review; Fjall MUST NOT be linked into `riffdbd` during the POC.

## 17.9 POC acceptance test

The final acceptance test is a scripted, self-verifying scenario:

1. Start `riffdbd` on an empty directory.
2. Bootstrap the first local human operator through retained loopback gRPC
   credentials, then create a restricted agent capability through the ordinary
   authenticated service path.
3. Validate and deploy the budget contract.
4. Verify the MCP client receives or can refresh the generated `allocatebudget` tool.
5. Seed an annual budget of 100 through a contract command.
6. Start two concurrent 80-unit allocation calls using different idempotency keys.
7. Assert exactly one `Allocated` outcome and one `InsufficientBudget` outcome.
8. Assert the committed allocation is 80 and the invariant remains true.
9. Invoke a third command with a failpoint that kills the connection or process after durable commit but before response.
10. Restart and retry the same idempotency key; assert the original outcome returns with replay metadata and no duplicate event.
11. Query the persisted outcome through MCP.
12. Query `BudgetUtilizationDaily` after the returned sequence and assert the projection includes the allocation.
13. Inspect provenance and verify actor, agent session, contract version, plan hash, command, outcome, and affected budget.
14. Attempt an unauthorized command and confirm it is absent from the MCP catalog and denied if invoked by stale name.
15. Run an integrity scan and clean shutdown.

A machine-readable test report MUST map each assertion to `POC-001` through `POC-010`.

---

# 18. Build, CI, security, and release engineering

## 18.1 Toolchain policy

- Pin the Rust toolchain in `rust-toolchain.toml`.
- Commit `Cargo.lock` for all binaries and the workspace.
- Workspace edition and minimum supported Rust version MUST be explicit.
- POC CI runs on Linux; macOS developer builds SHOULD work. Windows support is not a POC gate.
- First-party crates use `#![forbid(unsafe_code)]` unless a reviewed ADR grants an exception.

## 18.2 Required CI jobs

| Job | Gate |
|---|---|
| Formatting | `cargo fmt --check` |
| Static analysis | `cargo clippy --workspace --all-targets --all-features -- -D warnings` |
| Unit and integration | `cargo test --workspace --all-features` |
| Documentation | `cargo doc --workspace --no-deps` with warnings denied where practical |
| Dependency policy | `cargo deny check` for advisories, bans, licenses, and source policy |
| Supply-chain audit | `cargo audit` or equivalent advisory scan |
| Unused dependencies | `cargo machete` or equivalent |
| Fuzz smoke | Bounded fuzz execution for critical decoders and parser |
| Concurrency | Loom and Shuttle suites in dedicated profiles |
| Recovery | Process-kill matrix on a supported filesystem |
| MCP conformance | Initialization, tools, resources, progress, cancellation, and transport tests; Inspector smoke run |
| Reproducible demo | Build release binaries and run the acceptance script from a clean directory |
| Artifact generation | Rebuild Protobuf, JSON Schema, generated SDK, and docs; fail on uncommitted diff |

## 18.3 Dependency policy

Every new production dependency requires:

- Purpose and owning crate.
- License compatibility.
- Maintenance and release activity review.
- Unsafe-code and native-code assessment.
- Feature selection with default features disabled when appropriate.
- Security advisory status.
- An exit strategy when the dependency sits on a critical boundary.

Storage, consensus, cryptography, serialization, and MCP dependencies require an ADR or explicit inclusion in the architecture baseline.

## 18.4 Threat model for the POC

Primary threats:

- Unauthorized mutation through gRPC, MCP, CLI, or administrative paths.
- Tool-catalog overexposure that reveals commands or schema fields outside capability scope.
- Prompt or tool-output injection through user-controlled text.
- Idempotency-key collision or cross-principal replay.
- Malformed contract or record input causing resource exhaustion.
- Sensitive values leaking through diagnostics, metrics, provenance, or error payloads.
- Confused-deputy behavior when an agent supplies a source commit, approval ID, or tenant selector.
- Unbounded scans or waits causing denial of service.
- Corrupted or incompatible storage opened without safe detection.

The POC security bar is local-development safe, not internet-production ready. Streamable HTTP remains loopback-only by default until MVP authorization and deployment controls exist.

## 18.5 Backup and restore

The POC MUST support an offline consistent backup command. The backup includes:

- Storage-engine data files or a verified logical snapshot.
- Format version.
- Active contract version and bundle hashes.
- Last commit sequence.
- Integrity checksums.
- Build metadata.

Restore MUST refuse to overwrite a non-empty directory without an explicit destructive flag. A restored database MUST pass an integrity scan before readiness.

WP-070 owns the immutable redb artifact and manifest mechanics. Accepted
ADR-0050 assigns the operator-facing boundary to WP-155 after WP-185 and before
WP-190/WP-200. Exactly three API-neutral/public operations create, restore, and
read one maintenance operation; only gRPC, the Rust SDK, and CLI expose them.
Public input contains a checked operation ID and 1..=64-byte `BackupNameV1`,
never a path. The server resolves it beneath a configured absolute
`backup_root`; `.maintenance` is reserved and unreachable by that grammar.

Backup/restore is exclusive. The server drains accepted work under a fixed
deadline, closes redb, performs the backend operation, and runs complete
post-operation structural/catalog validation before readiness. No protected
request or polling is served while offline. Healthy replacement requires
current-database authorization before drain and independent staged-backup
authentication/authorization before publication. Empty/corrupt recovery has no
current authorization and requires the staged decision. The ordinary bearer is
retained only in an auth-owned bounded zeroizing handoff and is never persisted.

Every nonempty target requires exact wire confirmation
`ALLOW_REPLACE_NONEMPTY_TARGET`; the CLI spelling is
`--confirm-replace-current-database`. Confirmation grants no permission. The
external `backup_root/.maintenance` receipt is versioned, checksummed, bounded,
atomically replaced, and the sole audit/uncertainty exception for these offline
operations. It does not add a database metadata category, stored service-audit
row, redb key, or backup artifact.

Restore preserves `DatabaseId` and rewinds both authoritative sequence spaces
to the backup. Every observation/locator/idempotency/audit assumption from the
destroyed suffix is invalid, and a destroyed application or administration
sequence may later be reused for a different record. The POC has no
incarnation/history-epoch fence; documentation and destructive confirmation
must state that limitation.

Online backup, point-in-time recovery, incremental backup, encrypted backup, and remote object storage are MVP work.

## 18.6 Release artifacts

POC release artifacts:

- Exactly `riffdbd`, `riffdb`, and `riffdb-mcp`.
- A root README, installation/upgrade/removal instructions, and hardened
  systemd units whose stop behavior exercises SIGTERM and bounded graceful
  drain without stdin.
- Checksums and a software bill of materials.
- Example contracts and demo script.
- Generated Rust SDK example.
- Configuration reference.
- Storage and protocol compatibility statement.
- Known limitations and security posture.

Release builds MUST embed semantic version, Git revision, Rust version, enabled features, storage format version, contract IR version, and MCP protocol baseline.

---

# 19. Agent execution model and work packages

## 19.1 Handoff principles

The repository is intended to be built by multiple coding agents under human architectural control. Work MUST be decomposed so agents can make locally testable changes without silently changing cross-cutting semantics.

Each work package contains:

- A stable work-package ID.
- Objective and non-goals.
- Input ADRs and requirement IDs.
- Allowed crate and file scope.
- Public interfaces to implement or consume.
- Acceptance tests and commands.
- Required documentation and generated artifacts.
- Dependencies and unblock conditions.
- Explicit human review triggers.

Agents MUST NOT reinterpret normative requirements from issue summaries. This specification and accepted ADRs are authoritative.

## 19.2 Cross-cutting implementation rules

1. No API-specific direct storage access.
2. No command mutation outside the commit coordinator.
3. No nondeterministic OS clock or random access in the command runtime.
4. No new durable field without compatibility and recovery tests.
5. No public protocol change without schema fixtures and compatibility classification.
6. No unsafe Rust without an approved ADR.
7. No hidden administrative bypass.
8. No TODO that weakens correctness or authorization without a tracked issue and explicit safe failure behavior.
9. Generated files MUST be reproducible from checked-in sources.
10. Every PR description MUST list satisfied requirement IDs and executed test commands.
11. Because the root is a virtual Cargo workspace, every root-level integration
    test MUST be registered as an explicit `[[test]]` target in exactly one
    owning crate manifest and invoked by a package-qualified acceptance command.
12. `Cargo.lock` path authority permits generated root-workspace dependency
    resolution only; it does not approve a new crate, version, feature, build
    script, native/unsafe surface, or license without its ordinary review.
13. WP-020, the exact ADR-0012 WP-010 public-error carve-out, WP-065, WP-127,
    the exact narrow WP-137 additive public-protocol bridge, and ADR-0050's
    exact narrow WP-155 maintenance extension are the only schema-owner phases.
    WP-137 owns only ADR-0040's reviewed six RPCs/messages,
    two locator fields, two operation-schema sources, generated artifacts, and
    compatibility fixtures; it owns no durable, contract/compiler/bundle, or
    unreviewed public field. WP-155 owns only the three final AdminService RPCs,
    their messages and checked maintenance values; it owns no existing public
    or durable field and no MCP schema. The carve-out may add no request,
    result, service, RPC, or durable-record field. WP-060/WP-070 cannot guess
    durable fields, and WP-120/WP-130 cannot guess public fields or move service
    conversion into `riffdb-proto`.

## 19.3 Work-package dependency graph

[Graphviz source — agent work-package dependency graph](diagrams/work_package_dag.dot)

The graph uses hard dependencies. Parallel work is encouraged only after shared types and interface contracts are merged.

## 19.4 POC work packages

| ID | Name | Depends on | Primary deliverable | Exit gate |
|---|---|---|---|---|
| `WP-000` | Repository foundation | None | Cargo workspace, toolchain, CI, ADR template, contribution rules, deterministic generation checks | Clean CI on skeleton workspace |
| `WP-010` | Core types and error taxonomy | WP-000 | Nonzero IDs/versions, money/decimal type, time type, safe errors, canonical value model, and the exact ADR-0012 public-error slice | Serialization, canonicalization, error-schema generation, and zero-rejection properties pass |
| `WP-020` | Protobuf and durable envelope | WP-010 | Public/internal `.proto`, Protox/Prost build, versioned stored-record envelope | Golden compatibility fixtures pass |
| `WP-030` | Contract syntax | WP-010 | Logos lexer, LALRPOP grammar, source spans, AST | Parser corpus, diagnostics, fuzz smoke pass |
| `WP-040` | Typed IR and compiler | WP-010, WP-030 | Name resolution, type checker, invariant/outcome/dependency plans, JSON Schema | Budget contract compiles; invalid corpus rejects with stable diagnostics |
| `WP-045` | Budget comparison baseline | WP-040 | Shared workload/oracle and isolated PostgreSQL implementation | Deterministic oracle and PostgreSQL correctness preflight pass |
| `WP-050` | Contract catalog | WP-020, WP-040, WP-060 | Bundle lookup, catalog state semantics, compatibility report, bounded active-lineage materialization proof, opaque command `Ready`/resource evidence and pure current-recheck API, exact resolved projection/event materialization, same-session historical IR validation, exact V1/V2 historical index partition derivation/migration context, sealed backend-branded instruction/completion/driver chain, and typed expected-version deployment operation | Exact chain/count/byte/proof boundaries, returned-evidence retention, raw-current recheck, entity/event null-fill masks, raw-event immutability, catalog semantics, positional root-prefix derivation, unforgeable migration instruction coverage, exact-end validation, and atomic storage-operation tests pass; final orchestration evidence is WP-100/WP-120/WP-170 |
| `WP-060` | Storage semantic API | WP-010, WP-020, WP-040 | Owned snapshots, dependencies, `EvaluatedCommand`/`CommitIntent`, fields-private encoded aggregate-cap result, exact `EventHash`, bounded derived-recovery scans and projection fences, codec-bound migration-row evidence/bounds and identity-only startup migration outcomes without catalog authority or public transition operations, semantic durable DTOs, typed persistence transitions, and in-memory reference implementation | Reference-model, exact-end/identity-only migration startup type-state, derived scan/fence, pre-sequence ordering/capacity, event-hash, and semantic conformance tests pass |
| `WP-065` | Durable semantic record schema | WP-020, WP-060 | Exact 27-readable/26-writable `riffdb.storage.v1` registries with isolated `StoredIndexEntryV2`, preserved 26-record fixtures, canonical envelopes/V2 bounds, migration evidence factories, descriptors/hashes/wire validation, and checked storage-owned mappings | Proto/storage registry, migration codec, size/hash, decoder-fuzz, and clean deterministic regeneration tests pass |
| `WP-070` | Redb storage engine | WP-020, WP-050, WP-060, WP-065 | Frozen canonical tables/keys, ordered coordinator transaction, named memory/redb ports implementing the catalog-owned backend-branded migration driver through private helpers, exact session-bound scans and atomic V1-to-V2 compare/rewrite, dormant ports/fresh validation, recovery, SHA-256 backup, and dependency-free benchmarks | Storage properties, sealed memory/redb migration conformance, bounded pages, no-transaction-during-catalog-evaluation evidence, restart failpoints, and core process recovery pass without claiming IR-aware readiness |
| `WP-075` | Fjall semantic comparison | WP-060, WP-070 | Isolated non-production Fjall adapter and unchanged conformance/benchmark harness against the accepted V2/migration interface | Conformance report and reproducible evidence pass, or explicitly records an adapter conformance failure without weakening RiffDB |
| `WP-080` | Deterministic command runtime | WP-040, WP-060 | Shared pure expression evaluator plus predicate, invariant, postcondition, and commit-check evaluation; lineage-normalized snapshot IR interpreter that never blindly fills omissions, fixed logical time and budget, dependencies, `EvaluatedCommand`, and closed post-snapshot execution faults; no contract state-machine IR/execution, provenance, or command randomness | Differential tests against model and missing-field fail-closed fixtures pass |
| `WP-090` | Conflict manager | WP-010 | Canonical multi-key exclusive acquisition, cancellation, metrics | Loom/Shuttle suites pass |
| `WP-100` | Commit coordinator and idempotency | WP-050, WP-070, WP-080, WP-090, WP-110 | Admission reservation, catalog readiness/rechecks, explicit durability, mutation-affected epochs, pre-sequence capacity reservation including exact V2 aggregate-cap classification, verified sequence-derived graph, atomic outcome/event/provenance/outbox commit, and typed control-plane operations | Dependency/evidence/recheck ordering, origin-specific capacity mapping, fatal codec cases, explicit durability, concurrency, pre-sequence failpoints, crash, and replay tests pass |
| `WP-110` | Authorization and capability service | WP-010, WP-070 | Opaque capability records, policy decisions, obligations, and typed create/revoke operations for later coordinator routing | Policy, digest, revocation-state, and fail-closed tests pass; final non-bypass evidence is WP-120 |
| `WP-120` | API-neutral application service | WP-050, WP-080, WP-100, WP-110 | Six operation-specific service traits, checked contexts/DTOs, pure input/partition/conflict preparation with fail-closed pre-admission arithmetic, catalog-lineage limit validation mapping, current policy, audit orchestration, obligations, bounded reads/waits/streams, and server-side cursors | In-process end-to-end, exact root limit-error mapping, and no-storage-bypass tests pass |
| `WP-125` | Service comparison adapter | WP-045, WP-120 | Budget workload adapter over the API-neutral service, consuming only V2-only `Ready`/`Clean` startup in its fixture | Shared oracle passes against in-process RiffDB and crossed/migration startup outcomes reject |
| `WP-127` | Public Protobuf API completion | WP-020, WP-120 | Complete all remaining supported `riffdb.v1` messages, preserve the WP-010 execution-failure slice, and freeze descriptors, schema hashes, wire validation, and golden/client fixtures; no service conversion code | Proto tests and clean deterministic regeneration pass |
| `WP-130` | gRPC server and Rust SDK | WP-020, WP-120, WP-127 | Tonic services/client plus minimal production composition, matching-outcome invocation of the catalog-owned migration driver without instruction/backend-helper access, V2-only post-migration startup join, concrete ID/clock/cursor/digest providers, explicit code-level `sync`, staged lifecycle, and real restart proof | Public API/component-graph conformance plus child-process temporary-redb V1-migration/bootstrap/deploy/budget/restart pass |
| `WP-137` | Public gRPC MCP parity bridge | WP-130 | Exact six-RPC/22-RPC public extension, conditional discovery and operation schemas, locator-form outcome parity, process generation, public-client methods/retries/error view/protected loader, and compatibility artifacts | Human-accepted schema bytes; descriptor/wire generation, all conversions/client round trips, locator parity, retry/credential, fuzz, and architecture tests pass |
| `WP-135` | Public SDK comparison adapter | WP-125, WP-130, WP-137 | Public gRPC budget adapter plus exact `riffdb-budget-public` runner and `riffdb.budget.public-run/v1` process protocol | Shared oracle and isolated public-runner process fixtures pass through the canonical client path |
| `WP-139` | Budget safety counterexample evidence | WP-045, WP-135 | Isolated PostgreSQL negative controls, public-RiffDB contrasts, exact `riffdb.budget.safety-evidence/v1` report, and non-shipped runner | Four live scenarios, exact fixtures, architecture boundaries, and fail-closed demo pass without benchmark contamination |
| `WP-140` | Native MCP interface | WP-040, WP-120, WP-130, WP-137 | Pinned-rmcp stdio-over-gRPC and hosted-HTTP adapters, exact dynamic/fixed tools and resources, schemas, sessions, observers, rate/bounds, progress/cancellation | Separately accepted fixed/resource fixture bytes, MCP conformance, parity, authorization, generated-tool, limit, and dependency tests pass |
| `WP-150` | CLI and local developer flow | WP-130, WP-135, WP-137 | Public-only `riffdb`, exact bounded configuration/credentials/retries, checked budget runner, and reviewed JSONL/human output | Interface-only fixture acceptance and all public acceptance operations execute without internal APIs |
| `WP-160` | Durable outbox | WP-100, WP-120 | Complete fenced undelivered scan, delivery state, test connector, retry and duplicate simulations | Recovery exact-end, crash, and duplicate-delivery tests pass |
| `WP-170` | Projection core | WP-050, WP-100, WP-120 | Catalog-normalized event consumers using the shared pure evaluator, atomic state fences, count/sum state, durable frontier, rebuild, and read-after-sequence query | Ancestor-event materialization, atomic fence, prefix, restart, rebuild, and frontier properties pass |
| `WP-180` | Observability and diagnostics | WP-120, WP-140, WP-160, WP-170 | Closed owner telemetry, in-process metrics, health, explain output, pre-subscriber redaction | Required telemetry inventory is present without sensitive or unbounded labels |
| `WP-185` | Server composition | WP-130, WP-140, WP-160, WP-170, WP-180 | Extend the runnable WP-130 `riffdbd` graph with optional loopback MCP HTTP, signal-safe lifecycle, outbox, projection, observability, and derived health without replacing P1 startup/lifecycle providers | Extended composition, SIGTERM/stdin-EOF, and no-destination tests pass while retaining the WP-130 restart proof |
| `WP-155` | Public offline backup and restore maintenance | WP-070, WP-120, WP-137, WP-150, WP-185 | Three API-neutral/public operations, current/staged authorization, exclusive lifecycle, checked external receipt ledger, destructive confirmation, full restored-readiness proof, SDK/CLI integration, and rewind documentation | Public fixtures, authorization, receipt/failpoint, quiescence, restore-recovery, identity/rewind, and process tests pass |
| `WP-190` | Integrated crash harness | WP-070, WP-100, WP-155, WP-160, WP-170, WP-185 | Named failpoints, process controller, recovery assertions including synchronized post-commit/pre-response TCP loss, offline maintenance, and process termination/restart | Full cross-component and maintenance failpoint matrices plus same-key public replay identity/uniqueness assertions pass |
| `WP-200` | POC acceptance and release | All POC packages | One-command demo, benchmark report, security notes, release artifacts | POC-001 through POC-010 signed off |

WP-045, WP-075, WP-125, WP-135, and WP-139 are evidence tracks rather than
dependencies of the production semantic kernel. They may not add PostgreSQL or
Fjall dependencies to the main workspace. WP-200 consumes their reports and
runners, but a failure in a comparison implementation does not weaken or
redefine RiffDB semantics.

## 19.5 Recommended parallelization waves

These are dependency waves, not schedule commitments.

| Wave | Work that may proceed in parallel |
|---|---|
| A | WP-000 |
| B | WP-010 |
| C | WP-020, WP-030, WP-090 |
| D | WP-040 |
| E | WP-045, WP-060 |
| F | WP-050, WP-065, WP-080 |
| G | WP-070 |
| H | WP-075, WP-110 |
| I | WP-100 |
| J | WP-120 |
| K | WP-125, WP-127, WP-160, WP-170 |
| L | WP-130 |
| M | WP-137 |
| N | WP-135, WP-140 |
| O | WP-139, WP-150, WP-180 |
| P | WP-185 |
| Q | WP-155 |
| R | WP-190 |
| S | WP-200 |

Within a wave, agents MUST coordinate changes to shared types through interface PRs before parallel implementation PRs.

Accepted ADR-0050 activates WP-155 only after WP-185 has proved the complete
process lifecycle. WP-190 and WP-200 depend on it. Its cross-owner interface
changes must land as one reviewed checkpoint before offline driver work begins.

## 19.6 Human review triggers

A human architecture or security review is mandatory when a change:

- Modifies transaction ordering, read validation, conflict ownership, or commit atomicity.
- Adds an invariant category or command-language control-flow construct.
- Changes idempotency identity or outcome persistence.
- Adds or changes a durable record field or storage key layout.
- Changes MCP mutating-tool exposure, authorization, or output redaction.
- Adds unsafe Rust, native code, cryptography, or a critical dependency.
- Weakens a fail-closed behavior.
- Changes projection frontier semantics.
- Introduces a generic write, repair, or storage access path.
- Alters POC/MVP scope or accepts a deferred feature into the critical path.

## 19.7 Pull request contract

Every implementation PR MUST contain:

```text
Work package:
Requirement IDs:
ADRs consulted:
Behavior added or changed:
Compatibility classification:
Security implications:
Tests executed:
Generated artifacts checked:
Known limitations:
Follow-up issues:
```

A PR is incomplete if its acceptance test is described but not automated, unless the work package explicitly defines a manual review artifact.

## 19.8 Agent prompt template

```text
You are implementing <WORK_PACKAGE_ID> in the RiffDB workspace.

Authoritative inputs:
- Technical specification version 0.6
- Accepted ADRs: <LIST>
- Requirements: <LIST>
- Upstream interfaces at revision: <COMMIT>

Scope:
- You may modify: <PATHS>
- You may not modify: <PATHS OR SEMANTIC BOUNDARIES>

Required deliverables:
- <DELIVERABLES>

Acceptance commands:
- <COMMANDS>

Stop and request human review if:
- <TRIGGERS>

Do not weaken a correctness or security requirement to make a test pass. Add a failing test and report the blocking design conflict instead.
```

---

# 20. Roadmap from POC to MVP

## 20.1 Roadmap principles

- Advancement is gated by evidence, not a calendar date.
- A later stage may begin exploratory work, but it cannot become the supported path until the previous gate passes.
- Compatibility commitments increase by stage; POC formats and APIs may change, but changes still require migration tests.
- Replication follows proven single-node semantics. Distribution does not redefine command behavior.
- Native MCP remains generated from and subordinate to the contract and authorization model at every stage.

[Graphviz source — POC-to-MVP roadmap](diagrams/roadmap.dot)

## 20.2 Stage P0 — executable semantics

**Objective:** Prove that the contract language, type system, command outcomes, conflict plans, and deterministic runtime are coherent before durable integration dominates development.

**Deliverables:**

- Budget contract parses and type-checks.
- Compiler emits versioned IR, JSON Schemas, execution plan, and diagnostics.
- In-memory reference model executes commands.
- Generated histories preserve the budget invariant.
- Generated MCP tool metadata can be inspected from a bundle without a running database.

**Gate P0:** The same contract produces deterministic plans and results across repeated builds and model executions.

**P0 work-package members:** WP-030, WP-040, WP-060, and WP-080.

The long-lived budget comparison workload, deterministic oracle, and PostgreSQL
baseline begin after the compiler as a non-production evidence track. They are
not P0 gate members. After the public adapter exists, WP-139 adds correctness-
only counterexample evidence without becoming a P1 or P2 gate member. Their
public-adapter, safety-report, and benchmark evidence must be complete before
WP-200 closes P2.

## 20.3 Stage P1 — durable standalone POC

**Objective:** Prove durable command-only mutation, concurrency safety, idempotent uncertainty recovery, provenance, outbox intent, and crash recovery.

**Deliverables:**

- Redb-backed state and commit records.
- WP-065-reviewed durable semantic-record schemas and storage-owned mappings
  before redb persistence.
- Logical conflict manager and commit revalidation.
- Persistent idempotency outcomes.
- Contract catalog and atomic deployment.
- Minimal production `riffdbd` composition over redb, catalog, commit,
  authentication, policy, the API-neutral service, and Tonic, including concrete
  ID/clock/cursor/digest providers, an explicit code-level `sync` coordinator
  durability value with no constructor/backend default, and staged same-session
  startup validation.
- gRPC, Rust SDK, and CLI, including WP-137's reviewed six-operation parity
  bridge (exactly five services and 22 RPCs), operation schemas, locator-form
  outcome recovery, protected credential loader, and operation-specific retry
  helpers before MCP or CLI consume them.
- WP-150's 12-command public developer flow and reviewed JSONL surface; public
  backup/restore remains a mandatory POC-exit deliverable owned by the accepted
  post-WP-185 WP-155 boundary rather than being invented in P1.
- WP-127-reviewed complete public schemas before gRPC service/wire conversion.
- Atomic durable outbox intent as part of every applicable command commit; external dispatch remains P2.
- Core transaction-path process failpoints and integrity scan for redb and the commit coordinator.

**Gate P1:** All transaction-path and recovery properties pass under the budget
acceptance workload and generated histories. The first runnable checkpoint is a
real child process, not injected composition: start `riffdbd` on a temporary redb
database; create and durably retain the bootstrap credential; bootstrap and
deploy the canonical budget contract; execute `CreateBudget`; query the entity;
execute `AllocateBudget`; query the changed entity and resolve the persisted
outcome; stop cleanly; restart against the same database; query the same entity
and outcome again; and replay bootstrap using the retained credential to recover
the original capability result without echoing or reissuing the token and with
only the specified linked replay-audit records. Every database operation in this
sequence uses public gRPC or the public
CLI, never an in-process storage/service shortcut. It does not wait for MCP,
external outbox dispatch, projection workers, observability aggregation, or
WP-185.

**P1 work-package members:** WP-050, WP-065, WP-070, WP-090, WP-100,
WP-110, WP-120, WP-127, WP-130, WP-137, and WP-150. WP-065, WP-127, and
WP-137 are explicit gate members as well as hard dependencies of later packages;
they do not replace any previously listed member.

WP-139 begins only after WP-135 and remains a non-gating comparison track. Its
operational pause does not add a semantic dependency to WP-140 or WP-150.

## 20.4 Stage P2 — native agent interface and projection proof

**Objective:** Demonstrate that agents can safely discover and invoke contracts as dynamic MCP tools and reason about durable outcomes and derived state.

**Deliverables:**

- Stdio and loopback Streamable HTTP MCP.
- Generated command tools with input/output schemas.
- Contract, command-plan, outcome, commit, provenance, projection, and health resources.
- Capability-filtered tool catalog and repeated authorization.
- External outbox dispatch with duplicate and recovery evidence.
- Projection filter/count/sum engine with a durable frontier.
- Extension of the runnable P1 `riffdbd` graph with MCP HTTP, derived workers,
  observability/derived health, and the full cross-component crash matrix.
- Public offline backup/restore through WP-155's API-neutral gRPC/SDK/CLI
  boundary, checked receipt recovery, staged-backup authorization, and explicit
  destructive-rewind limitation.
- Consumption of WP-139's checked safety report with deliberately unsafe
  PostgreSQL variants excluded from every performance result.
- Full POC demo and handoff documentation.

**Gate P2 / POC exit:** `POC-001` through `POC-010` pass; known limitations are explicit; architecture review confirms the semantics are worth productizing.

**P2 work-package members:** WP-140, WP-160, WP-170, WP-180, WP-185,
WP-155, WP-190, and WP-200.

## 20.4.1 Stage P3 — safe symbolic application platform

**Objective:** Replace storage-shaped application plumbing with bounded
symbolic reads, generated named operations, one-snapshot page execution, and
closed safe application authority.

**Gate P3:** WP-205 through WP-300 pass. TicketDesk uses exact named queries and
compiled commands without caller-visible numeric IDs, encoded keys, field
masks, public N+1 reads, raw kernel authority, undeclared supported integrity,
or per-step budget amplification.

## 20.4.2 Stage P4 — Agent Application Alpha

**Objective:** Prove that a fresh coding agent can build an unfamiliar
application from an empty repository using only public textual, generated, CLI,
and MCP surfaces.

**Deliverables:**

- Complete generated Rust and TypeScript operation clients with no handwritten
  transport/encoding/decoding glue.
- One symbolic application manifest and symbolic roles compiled into exact
  named-operation authority.
- Bounded structured application errors preserved across gRPC, Rust,
  TypeScript, CLI, and MCP.
- Resumable concurrent command batches for ordinary seed/import workflows.
- `riffdb new`, one canonical `riffdb dev`, an application-only default facade,
  and mandatory kernel-boundary linting.
- A real TypeScript web application with the same semantic fixtures as Rust.
- Blog/CMS and orders/inventory evidence before any additional RiffQL
  construct is accepted.
- Four sealed fresh-agent evaluations covering both domains in both languages.

**Gate P4 / Agent Application Alpha:** Every `AAA-*` requirement passes. Each
sealed run completes without human product workaround, kernel use, handwritten
RiffDB glue, implementation-source access, or unresolved required query shape;
reaches its first write within 30 minutes and first page-shaped read within
60 minutes; and rates the experience at least 8.5/10.

**P4 work-package members:** WP-305, WP-310, WP-315, WP-320, WP-325,
WP-330, WP-335, WP-340, WP-345, WP-350, WP-355, WP-360, WP-362,
WP-364, WP-365, WP-366, and WP-370.

## 20.5 Stage A — single-node alpha hardening

**Objective:** Turn the prototype into a stable, supportable single-node alpha for trusted design partners.

**Additions:**

- Stable storage-format migration framework.
- Online consistent backup and verified restore.
- Contract language versioning and bundle signing.
- More entity indexes and bounded indexed reads.
- Operational repair commands with two-person or approval policy and complete audit.
- Remote MCP over TLS with standards-based authorization.
- Production-hardened TypeScript compatibility plus generated Python clients.
- Outbox connector framework with destination-specific idempotency configuration.
- Projection backfill, rebuild, and operator controls.
- Resource quotas and workload admission.
- Upgrade, downgrade, and rolling client-compatibility tests.

**Gate A:** A design partner can run a non-critical workload with documented backup, restore, upgrade, access-control, and incident procedures.

## 20.6 Stage B — replicated beta

**Objective:** Preserve the same transaction and contract semantics through node failure.

**Additions:**

- OpenRaft or equivalent behind `riffdb-replication`.
- Replicated log entry format derived from normalized commit commands or records.
- Deterministic state-machine application.
- Snapshots, membership changes, node bootstrap, and catch-up.
- Leader routing and client retry metadata.
- Fault-injection cluster tests and model checking for critical algorithms.
- Backup coordination and disaster recovery procedures.

**Constraints:**

- One replicated group may host all data initially.
- No automatic sharding is required at this stage.
- A leader change MUST not alter idempotency or typed-outcome semantics.

**Gate B:** The cluster passes repeated leader-loss, network-partition, snapshot-installation, and client-uncertainty tests without violating committed-state or outcome guarantees.

## 20.7 Stage C — partitioned MVP

**Objective:** Offer a usable early product for tenant-local operational workloads and autonomous agents.

**MVP capability set:**

- Replicated tenant-local partition leaders.
- Explicit partition routing and placement epochs.
- Command compiler rejects undeclared cross-partition access.
- Tenant movement with fencing, catch-up, routing switch, and rollback plan.
- Production-grade OAuth 2.1 protected-resource behavior for remote MCP and APIs.
- Field and tenant policy, capability expiration, approval obligations, audit export, and rate limits.
- Stable Rust, TypeScript, and Python command clients.
- Native MCP command tools, resources, prompts, and long-operation progress.
- Durable projections with bounded supported joins, backfill, rebuild, watermarks, and operational SLO reporting.
- Agent contract branches with representative or transformed data, semantic contract diff, replay, and approval workflow.
- Backup, restore, upgrade, observability, incident tooling, and documented support boundaries.
- Open CDC and Parquet export; read-only SQL exploration may be included if it cannot weaken command-only writes.

**MVP exclusions:**

- General cross-partition ACID transactions.
- Multi-region synchronous writes.
- Global uniqueness with hidden consensus cost.
- Arbitrary incremental analytics.
- Automatic inference of invariants.
- Merging arbitrary divergent transactional branches.

**Gate C / MVP:** At least one representative tenant-local production workload meets published correctness, recovery, security, upgrade, and operational acceptance criteria under failure injection and sustained load.

## 20.8 Post-MVP research tracks

- Explicit `DistributedTx` and saga semantics.
- Escrow and commutative data types.
- Global index and uniqueness services with visible cost.
- Multi-region placement and bounded-staleness policies.
- Rich incremental joins, windows, approximate aggregates, and Arrow/DataFusion query execution.
- Iceberg export and external analytical catalog integration.
- Automated contention-aware plan alternatives with versioned proofs and operator approval.
- Verified migration synthesis and higher-assurance invariant analysis.

---

# 21. Risk register

| ID | Risk | Probability | Impact | Leading indicator | Mitigation / decision gate |
|---|---|---|---|---|---|
| `R-01` | Contract DSL becomes a general programming language | Medium | High | Requests for loops, callbacks, arbitrary I/O, or host-language escape hatches | Keep bounded expression language; require new constructs to preserve static dependency analysis; use durable workflows outside commands. |
| `R-02` | Declared conflict keys are too coarse and cause hot-key serialization | High | High | Lock wait and queue concentration on a small key set | Instrument from POC; separate business aggregate from conflict domain; evaluate optimistic or escrow plans only after semantics are proven. |
| `R-03` | Declared conflict keys are too narrow and miss invariant dependencies | Medium | Critical | Model or replay finds histories that violate an invariant | Compiler dependency checks, runtime read revalidation, adversarial corpus, fail closed on undeclared mutable dependency. |
| `R-04` | Embedded engine behavior leaks into public semantics | Medium | High | Application service depends on redb transaction types or single-writer quirks | Enforce semantic storage trait; maintain in-memory model; benchmark Fjall at POC exit. |
| `R-05` | Commit sequencing of persisted business rejections reduces throughput or confuses users | Medium | Medium | Large log share from no-op outcomes or operator confusion | Measure; keep terminal admission semantics explicit; consider a separately ordered outcome journal only through ADR. |
| `R-06` | MCP tool catalog is too large or unstable for clients/models | Medium | Medium | Tool-list size, frequent list changes, poor tool selection | Capability filtering, command namespaces, concise descriptions, pagination where supported, optional domain servers, generated prompts. |
| `R-07` | MCP becomes a privileged bypass | Low | Critical | API-specific storage calls or weaker authorization | Shared application service, repeated authorization, stale-name denial tests, architecture checks. |
| `R-08` | Model-facing text leaks sensitive data or embeds malicious instructions | High | High | Untrusted free text in descriptions, errors, or resources | Static generated descriptions, redaction, output classification, structured content, bounded Markdown rendering. |
| `R-09` | Protobuf or storage format evolves without a safe migration path | Medium | Critical | Field reuse, implicit defaults, fixtures fail | Reserve field numbers, version envelope, compatibility fixtures, staged migration framework before alpha. |
| `R-10` | Crash semantics around idempotency are incorrect | Medium | Critical | Duplicate events or uncertain outcomes in failpoint tests | Store outcome, mutations, events, and provenance in one atomic transaction; process-kill matrix is a POC gate. |
| `R-11` | Projection frontier advances incorrectly | Medium | High | Query-after-sequence returns missing data | Atomic projection state/frontier updates, idempotent apply, prefix property tests. |
| `R-12` | All-Rust requirement admits unreviewed unsafe/native transitive code | Medium | Medium | Dependency audit shows native build scripts or broad unsafe surface | Cargo-deny policy, unsafe/native inventory, explicit exceptions and alternatives. |
| `R-13` | Raft integration later forces semantic redesign | Medium | High | Commit intent includes process-local state or nondeterminism | Replicated-state-machine-shaped interfaces now; normalized durable entries; deterministic apply tests before replication. |
| `R-14` | Agent parallelism creates incompatible subsystem assumptions | High | High | Repeated merge conflicts and duplicated semantic types | Work-package DAG, interface-first PRs, stable requirement IDs, ADR gates, generated contract tests. |
| `R-15` | POC scope expands into SQL, general analytics, or distribution | High | High | Critical path includes optimizer, Raft, or joins before POC gate | Enforce scope labels and MVP exclusions; human review for scope changes. |
| `R-16` | Destructive restore is mistaken for continuous history under the preserved `DatabaseId` | Medium | Critical | A client reuses a pre-restore locator or assumes a destroyed sequence/idempotency suffix cannot recur | Exact confirmation and documentation, invalidate all process observations, stage and fully validate restore, expose the limitation in release evidence, and require an incarnation ADR before stronger claims. |

Risk owners are assigned in the project tracker. A risk may be closed only with evidence or an accepted product decision, not by removing it from the register.

---

# 22. Architecture decision records and open decisions

## 22.1 Required ADRs

Specification v0.36 records each ADR's current status. An Accepted record is
authoritative; a Proposed record remains planning input until its exact text
receives human review. Where this table and a work-package deadline differ, the
earlier deadline governs unless a reviewed reconciliation changes both sources.

| ADR | Status | Decision | Required before |
|---|---|---|---|
| `ADR-0001` | Accepted | Standalone database rather than PostgreSQL extension or control plane | P0 gate |
| `ADR-0002` | Accepted | Canonical bounded contract grammar, typed IR, versioning, deterministic bundle, and explicit POC deferral of contract state-machine source/IR/execution | WP-030 grammar freeze and WP-040 interface |
| `ADR-0003` | Accepted | Logical pessimistic conflict ownership, dependency validation, and commit ordering | WP-060/WP-090 interfaces |
| `ADR-0004` | Accepted | Coordinator-driven owned-snapshot semantic storage API, pre-sequence write-plan/capacity ordering, and exact redb 4.1.0 baseline | WP-060 interface |
| `ADR-0005` | Accepted | Idempotency identity, pending reservation, persisted outcomes, and sequence semantics | WP-060 key freeze and WP-100 |
| `ADR-0006` | Accepted | Phased single-owner Protobuf schemas, exact values, durable envelope, and compatibility policy | WP-020 schema implementation |
| `ADR-0007` | Accepted | Shared policy-filtered application service, executor/read ports, cursors, and durable invocation audit for gRPC, MCP, CLI, and SDK | WP-100 coordinator boundary and WP-120 interface |
| `ADR-0008` | Accepted | Exact native MCP protocol/dependency/features, stdio-over-gRPC and stateful loopback HTTP topology, sessions/rate/bounds, canonical resources/outcome locators/cursors, schemas, discovery/watchers, and compatibility checkpoints | WP-137 and WP-140 interfaces |
| `ADR-0009` | Accepted | Opaque HMAC-digested, environment/database/audience-bound POC capabilities with stable-ID records, digest lookup, key custody, and recoverable one-time bootstrap | WP-060 storage interface and WP-110 implementation |
| `ADR-0010` | Accepted | Event-derived POC projection engine and frontier semantics | WP-070 projection persistence and WP-170 |
| `ADR-0011` | Accepted | Canonical values, fixed-scale decimals, keys, serialization, domain-separated hashing, and the exact durable-event hash preimage | WP-010 semantic types and WP-060 event boundary |
| `ADR-0012` | Accepted | Deterministic transaction context, durable logical time, no POC command randomness, and dependency-validated terminal execution-failure admission | WP-060 admission state and WP-080 implementation |
| `ADR-0013` | Accepted | Stable semantic IDs, canonical IR/bundle encoding, plan hashing, and schema generation | WP-040 interface |
| `ADR-0014` | Accepted | Projection-plan and contract-plan-root typed hash domains | WP-010 follow-up and WP-040 interface |
| `ADR-0015` | Accepted | Explicit binding-failure outcomes and command-only budget bootstrap | WP-030 follow-up and WP-040 fixtures |
| `ADR-0016` | Accepted | Canonical key components, typed partition identity, and complete index-key framing | WP-010 follow-up and WP-040 interface |
| `ADR-0017` | Accepted | Projection group keys, lineage/plan identities, nonzero generations, apply hashes, explicit frontiers, lifecycle, and atomic query/rebuild semantics | WP-040 projection schema and WP-060/WP-070 projection storage boundary |
| `ADR-0018` | Accepted | Pure UUIDv7 assembly, system-source ownership, database initialization, provenance generation/replay, and request/capability identifier boundaries | WP-010 foundation, WP-060/WP-070 storage, and WP-100/WP-130 providers |
| `ADR-0019` | Accepted | Exactly six authoritative POC metadata categories; durable node identity, clean-shutdown marker, and persisted integrity history deferred with unconditional startup validation | WP-060 semantic metadata API |
| `ADR-0020` | Accepted | MCP command tool-name normalization, compiler-owned versioned registry, collision rejection, and catalog activation revalidation | WP-040 compiled contract bundle |
| `ADR-0021` | Accepted | Exact lineage-scoped service-audit target variants/tags, canonical list ordering, and shared-service construction rule | Amended WP-010 audit vocabulary |
| `ADR-0022` | Accepted | Exact durable semantic Protobuf modules, 26-payload registry, field/tag/presence rules, canonical-wire validation, and storage-owned codec boundary | WP-065 implementation |
| `ADR-0023` | Accepted | WP-100 commit-evaluator dependency, bounded reevaluation, audit-input inversion, provenance attempts, v1 empty covered values, late commit-check arithmetic, absent revoke, and no-transition audit semantics | WP-060 correction, WP-100/WP-110/WP-120 implementation, and WP-130/WP-200 evidence |
| `ADR-0024` | Accepted | Canonical provenance resource locator and bounded redacted presentation | Provenance public presentation |
| `ADR-0025` | Accepted | Bootstrap/auth-to-service consumer boundary, closed capability-create invocation, and production construction ownership | WP-120 service boundary |
| `ADR-0026` | Accepted | Grammar-v1 tenant scope, two-stage authorization, outcome recovery, committed durability, and fail-closed incident source | WP-120 authorization/recovery implementation |
| `ADR-0027` | Accepted | Conservative service response accounting, whole-item byte-fitting, oversize disposition, and response validation | WP-120 response boundary and WP-137 discovery extension |
| `ADR-0028` | Accepted | Complete phase-zero public Protobuf inventory, tags/presence, validation, generation, and compatibility ownership | WP-127 public schema freeze |
| `ADR-0029` | Accepted | Structural public key envelopes followed by service-owned schema-directed validation | WP-120 submitted keys |
| `ADR-0030` | Accepted | Capability-partition startup evidence, catalog validation, ordering, and exact-end coverage | WP-050/WP-060/WP-070 startup |
| `ADR-0031` | Accepted | Structurally submitted command values and service-owned schema materialization | WP-120 input boundary |
| `ADR-0032` | Accepted | Same-session retained metadata handoff for lifecycle and readiness | WP-130 startup composition |
| `ADR-0033` | Accepted | First-durable-commit notification publication through a least-authority sink | WP-100/WP-130 subscription composition |
| `ADR-0034` | Accepted | Health-only staged application-service activation and move-only authority | WP-130 startup routing |
| `ADR-0035` | Accepted | Atomic authoritative index/commit scan fences and explicit before-first state | WP-070/WP-120 scans |
| `ADR-0036` | Accepted | Structural public query components and service-owned schema materialization | WP-120 query boundary |
| `ADR-0037` | Accepted | Exact foundational public-error/type dependencies for the Rust SDK | WP-130/WP-137 client |
| `ADR-0038` | Accepted | Partition-filtered authoritative index scans, V2 semantic destination, and restartable offline migration requirement | WP-050 through WP-130 correction |
| `ADR-0039` | Accepted | Isolated V2 descriptor, 27-readable/26-writable registries, codec/session-bound evidence, historical partition derivation, linear migration, and narrow capacity classification | WP-050 through WP-130 implementation and WP-190/WP-200 evidence |
| `ADR-0040` | Accepted | Six-RPC public parity bridge, conditional discovery/fences, operation schemas, locator outcome parity, server generation, public-client helpers, and WP-137 | WP-137 before WP-135/WP-140/WP-150 |
| `ADR-0041` | Accepted | Public-only CLI dependencies/credentials/retries/configuration/JSONL, exact budget runner, package sequencing, and the WP-155 reservation subsequently resolved by ADR-0050 | WP-135, WP-137, and WP-150 interfaces |
| `ADR-0042` | Accepted | Sealed catalog-owned, concrete-backend-branded index migration driver with identity-only storage API and private backend helpers | WP-050 through WP-130 correction and WP-190/WP-200 evidence |
| `ADR-0043` | Accepted | Auth-owned retained opaque MCP credential with bounded zeroizing copy and authentication-only borrow | WP-140 hosted HTTP credential handoff |
| `ADR-0044` | Accepted | Hosted MCP context construction, foundational adapter dependencies, current-thread stdio runtime, and hard rmcp telemetry suppression | WP-140 implementation and WP-185 composition |
| `ADR-0045` | Accepted | Isolated four-scenario budget safety counterexamples, qualified claim boundary, exact non-shipped report/runner, and WP-139 ownership | WP-139 and WP-200 evidence |
| `ADR-0046` | Accepted | Decimal precision, schema-bound names, compatibility metadata, complete resources, and hosted-only MCP limiting | WP-137 and WP-140 public presentation |
| `ADR-0047` | Accepted | Pre-release object-root correction for thirteen canonical MCP schemas | WP-140 schema fixtures and conformance |
| `ADR-0048` | Accepted | Separate logical observer operations from metered physical service calls and remove the stdio parity probe | WP-140 observer bounds and conformance |
| `ADR-0049` | Accepted | Bounded derived recovery scans/fences, catalog-owned event materialization, narrow projection evaluator ownership, closed owner telemetry, and hosted-MCP process composition | WP-160/WP-170/WP-180 correction and WP-185 composition |
| `ADR-0050` | Accepted | Three public offline maintenance operations, current/staged authorization, external checksummed receipts, exclusive restore lifecycle, and explicit history-rewind semantics | WP-155 and WP-190/WP-200 evidence |
| `ADR-0051` | Accepted | Formal bounded symbolic RiffQL with typed cardinality, deterministic indexed planning, same-partition locality, and compiler-derived authorization | WP-210 through WP-240 |
| `ADR-0052` | Accepted | Immutable exact-contract query modules, additive application gRPC, symbolic MCP/CLI operations, and optional generated clients | WP-250 and WP-260 |
| `ADR-0053` | Accepted | One-snapshot composite execution, epoch-bound cursors, rebuildable current catalog/capability views, and measured unary-gRPC-first optimization | WP-205, WP-240, and WP-270 |
| `ADR-0054` | Accepted | Bounded collection-to-complete-key dependencies, explicit missing-target outcomes, and one-snapshot dependent point batches | WP-275 |
| `ADR-0055` | Accepted | Disjoint stable-application, ad-hoc-agent, and kernel authority; private exact-plan proofs; whole-query fuel; declared relationship/uniqueness integrity; and negative safety canaries | WP-280 through WP-300 |
| `ADR-0056` | Accepted | Agent Application Alpha before operational/distributed alpha; complete generated bindings, symbolic roles, structured application errors, command batches, canonical scaffolding, TypeScript parity, evidence-driven RiffQL growth, and sealed independent evaluation | WP-305 through WP-340 |
| `ADR-0057` | Accepted | Compiler-owned exact application lock, bounded authoring diagnostics, empty-directory scaffold, complete public authoring kit, TypeScript/builder-MCP parity, durability-preserving performance investigation, rehearsals, canaries, and campaign 02 | WP-345 through WP-370 |
| `ADR-0058` | Accepted | Bounded typed FIFO writer scheduling, production group durability over redb `Immediate`, two-transition audited commands, complete-outcome release proof, and independent grouped uncertainty | WP-364 |
| `ADR-0059` | Accepted | Same-partition cross-aggregate read dependencies, exactly one mutation aggregate, mutation-only conflict keys, exact commit-time read revalidation, effective group evidence, and same-run PostgreSQL write parity | WP-366 |

## 22.2 Decisions to resolve before implementation reaches the named gate

ADR-0015 resolved initial entity creation: the canonical `CreateBudget` compiled
command seeds the demo through the ordinary coordinator path, and direct
storage/admin seeding remains forbidden. The accepted ADR-0004/ADR-0012 batch
and ADR-0023 also resolved the remaining grammar-v1 transaction defaults:

- Read-only commands are unjournaled and receive no durable idempotency record,
  persisted outcome/provenance, or application sequence; service audit is a
  separate outcome-free administration record.
- A command may observe multiple logical conflict domains only within its one
  declared partition. Every mutation domain is acquired up front and every
  influential observation is represented and revalidated.
- Write-influencing indexed range reads are rejected by grammar v1. The frozen
  prefix-epoch storage capability does not enable them; any future bounded IR
  requires accepted static target derivation and explicit up-front exclusion.
- Arithmetic and resource faults may terminalize as ADR-0012
  `ExecutionFailed` only after complete dependency equality; lineage expansion
  additionally requires exact raw-observation equality and deterministic
  reproduction of the same overflow. They receive no application sequence or
  durable command provenance and use the exact public error and uncertainty
  rules in that ADR. A late transaction-current
  commit-check arithmetic fault discards the already-sourced unpersisted
  provenance candidate and sources no second candidate while terminalizing.
- Grammar/IR-v1 coordinator writes always use the canonical empty record for
  index `covered_values`; nonempty covered values remain a future language/IR
  and durable-compatibility decision.
- Full reevaluation is bounded to three attempt slots per invocation, consumed
  before catalog snapshot materialization; valid lineage overflow consumes a
  slot without runtime entry.
  Exhaustion leaves Pending unchanged and maps to the existing public
  `ConcurrencyDeadlineExceeded` without adding a durable counter or error kind.
- POC contract compatibility is additive/documentation-only; removal of an
  entity, field, command, outcome, event, or projection is rejected.
- Optional-field evolution is executable only through the catalog's exact
  active-lineage proof: at most 4,096 bundles, 64 MiB summed canonical bundle
  bytes, and a 2 MiB process-local proof. Commit alone applies its ancestor-only
  null-fill masks before snapshot and transaction-current evaluation; runtime
  never infers ancestry or blindly fills a missing optional field.
- A command candidate assigns no sequence until private validation has derived
  mutation-affected epoch targets, read their current positions, frozen the exact
  sequence-free write plan, and reserved semantic plus conservative encoded
  capacity. Actual canonical envelopes are checked against the retained per-
  class bounds before staging.
- `EventHash` is the `riffdb.event/v1` hash of the exact EventId/EventTypeId/
  length-framed canonical `Value::Record` preimage in Section 10.4; no component
  may substitute payload-only or Protobuf hashing.

| Decision | Resolve by | Default in this specification |
|---|---|---|
| Which cryptographic provider is used for TLS in alpha/MVP? | Before remote MCP alpha | Deferred; requires dependency and platform ADR. |
| Redb versus Fjall for MVP | POC exit architecture review | Redb remains baseline unless workload and recovery evidence justify change. |

## 22.3 Open research questions

- Which useful cross-row invariant classes can the compiler prove local to a declared conflict domain?
- How should predicate dependencies be represented without forcing global serializable validation?
- When can a pessimistic plan be safely replaced by optimistic validation or escrow without changing declared outcomes?
- How should plan changes be diffed and approved when contention or availability behavior changes but logical results do not?
- What is the smallest branch representation that enables agent replay and semantic migration review without promising transactional-history merge?
- Which bounded join classes can be incrementally maintained with understandable recovery and memory behavior for MVP?

Open research does not block the POC unless it changes a requirement explicitly marked as a POC gate.

---

# 23. End-to-end demo contract and script

## 23.1 Contract source

```text
contract LegalSpend version 1 {
  entity Budget {
    key (organization_id: uuid, fiscal_year: i64)
    field approved_amount: decimal<28,2>
    field allocated_amount: decimal<28,2>
    field updated_at: timestamp

    invariant non_negative:
      allocated_amount >= 0.00

    invariant within_approval:
      allocated_amount <= approved_amount
  }

  event BudgetAllocated {
    organization_id: uuid
    fiscal_year: i64
    matter_id: uuid
    amount: decimal<28,2>
  }

  aggregate AnnualBudget {
    root Budget
    partition_by organization_id
    conflict_key (organization_id, fiscal_year)
  }

  command CreateBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input approved_amount: decimal<28,2>

    idempotency_key idempotency_key
    create Budget(organization_id, fiscal_year) as budget
      else BudgetAlreadyExists {
        organization_id: organization_id,
        fiscal_year: fiscal_year
      }

    require positive_approval: approved_amount > 0.00
      else InvalidApprovedAmount { minimum: 0.01 }

    set budget.approved_amount = approved_amount
    set budget.allocated_amount = 0.00
    set budget.updated_at = tx.time

    return BudgetCreated { budget: budget }
  }

  command AllocateBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input matter_id: uuid
    input amount: decimal<28,2>

    idempotency_key idempotency_key
    mutate Budget(organization_id, fiscal_year) as budget
      else BudgetNotFound {
        organization_id: organization_id,
        fiscal_year: fiscal_year
      }

    require positive_amount: amount > 0.00
      else InvalidAmount { minimum: 0.01 }

    require sufficient_budget:
      budget.allocated_amount + amount <= budget.approved_amount
      else InsufficientBudget {
        approved: budget.approved_amount,
        allocated: budget.allocated_amount,
        requested: amount
      }

    set budget.allocated_amount = budget.allocated_amount + amount
    set budget.updated_at = tx.time

    emit BudgetAllocated {
      organization_id: organization_id,
      fiscal_year: fiscal_year,
      matter_id: matter_id,
      amount: amount
    }

    return Allocated {
      budget: budget,
      remaining: budget.approved_amount - budget.allocated_amount
    }
  }

  projection BudgetUtilizationDaily {
    source event BudgetAllocated
    key (organization_id, fiscal_year, tx.date)
    measure allocated = sum(amount)
    frontier transactionally_ordered
  }
}
```

This source intentionally uses the canonical Section 7.2 grammar. There is no alternate module declaration, scalar alias, or explicit outcomes block.

## 23.2 Generated Rust call

```rust
let result = client
    .allocate_budget(AllocateBudgetInput {
        idempotency_key,
        organization_id,
        fiscal_year: 2026,
        matter_id,
        amount: Money::parse("80.00")?,
    })
    .await?;

match result.outcome {
    AllocateBudgetOutcome::Allocated { remaining } => {
        println!("committed at {} with {} remaining", result.commit_sequence, remaining);
    }
    AllocateBudgetOutcome::InsufficientBudget { .. } => {
        println!("allocation rejected by declared business rule");
    }
    AllocateBudgetOutcome::InvalidAmount { minimum } => {
        println!("amount must be at least {minimum}");
    }
}
```

## 23.3 Generated MCP call

```json
{
  "name": "riffdb.cmd.legalspend.allocatebudget",
  "arguments": {
    "idempotency_key": "budget-allocation-019bf6aa-7fb0-7aa5-8511-4f983a741e31",
    "organization_id": "019bf6aa-89a5-7785-91f8-16e7dcf50404",
    "fiscal_year": 2026,
    "matter_id": "019bf6aa-91c5-7405-a62b-313d2a65c92f",
    "amount": "80.00"
  }
}
```

Representative structured result:

```json
{
  "status": "committed",
  "commit_sequence": "42",
  "contract_version": 1,
  "plan_hash": "f63b14d3341eb0f8dd847f577994ded6372535ca4d71ec96dc6f20d9a3f9937a",
  "outcome": {
    "type": "Allocated",
    "remaining": "20.00"
  },
  "provenance_uri": "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23",
  "durability_mode": "sync",
  "outcome_uri": "riffdb://outcome/agent_01/legalspend/2/riffdb.cmd.legalspend.allocatebudget/AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
}
```

## 23.4 Demonstration narrative

The demo MUST make failure behavior visible, not only the success path. It shows:

- The contract source and generated explain plan.
- An empty database seeded with approved amount `100.00` through `CreateBudget`, never a direct storage or administration write.
- The same command invoked through Rust and MCP.
- Lock contention on the annual budget conflict key.
- A declared business rejection rather than a generic serialization exception.
- A connection-loss or crash boundary and safe idempotent recovery.
- One durable event and one projection update.
- A read-after-sequence query.
- A provenance trace and capability decision.
- An unauthorized agent seeing a smaller tool catalog.
- The four WP-139 counterexamples beside the preserved correct PostgreSQL
  control, with every "impossible" claim qualified to RiffDB's supported
  application mutation surface and every unsafe variant excluded from
  benchmarks.

The demo script exits non-zero when any assertion fails and writes a JSON report containing requirement IDs, command sequences, outcome types, and integrity status.

---

# 24. Definition of done

## 24.1 POC definition of done

The POC is done only when:

- All `POC-*` criteria pass in CI from a clean checkout.
- The budget acceptance demo passes through gRPC, Rust SDK, CLI, and MCP.
- The crash matrix covers every authoritative transaction boundary.
- Public offline backup/restore, receipt uncertainty recovery, staged
  authorization, and DatabaseId-preserving rewind limitations pass their
  process matrix and documentation checks.
- The reference-model and generated-history suites run without invariant divergence.
- Storage, protocol, contract IR, and MCP compatibility policies are documented.
- There is no undocumented mutation or administrative bypass.
- A threat model and dependency audit are published.
- Benchmark methodology and results are reproducible.
- The root README, installation/upgrade/removal instructions, and hardened
  systemd units are verified from the release bundle.
- Known limitations identify all conditions under which the database rejects a contract or cannot guarantee a property.
- The architecture review records one of three decisions: proceed to alpha, revise and repeat a gate, or stop because the core thesis was not demonstrated.

## 24.2 MVP definition of done

MVP is done only when:

- Replication and partition placement preserve POC command, idempotency, and outcome semantics under failure.
- Production authorization, remote MCP protection, audit, and field policy pass an independent security review.
- Backup, restore, upgrade, downgrade policy, and disaster recovery exercises are documented and tested.
- At least one representative tenant-local workload runs under sustained load and injected failure while meeting published SLOs.
- Contract and client compatibility are supported across independently deployed application versions.
- Projection backfill, rebuild, failure, and read-after-watermark behavior are operationally manageable.
- Agent branches and semantic change review cannot mutate production without policy and approval.
- Support boundaries and post-MVP exclusions remain explicit.

## 24.3 Application-platform milestone

The post-POC application milestone keeps the existing `riffdb.v1` surface as a
compatible, supported kernel protocol. Normal application and agent development
uses formal RiffQL, named operations, or clients generated from named
operations. Raw stable IDs, field masks, encoded index keys, and caller-built
access plans are not part of that primary experience.

RiffQL is read-only. Compiled contract commands remain the only application
mutation mechanism. Natural language may be translated to RiffQL by an agent,
but the database accepts only deterministic formal source and typed values.

The primary experience is also an authority boundary. Stable applications may
invoke only exact named compiled commands and exact named deployed queries.
Scoped agents may additionally receive explicit ad-hoc RiffQL authority. Raw
entity/index operations remain compatible kernel/administrative operations and
are never implied by either application profile. The normative product rule and
claim boundary are documented in `docs/safety-by-construction.md` and accepted
ADR-0055.

- `RQL-001`: The implementation MUST provide one versioned formal RiffQL
  grammar, formatter, canonical AST, and parser for both ad-hoc and named
  read-only queries. It MUST NOT admit mutation, SQL escape, natural-language
  interpretation, host callbacks, network/filesystem/clock/randomness access,
  unbounded loops, or recursion.
- `RQL-002`: RiffQL MUST express typed parameters and explicit `one`, `maybe`,
  and bounded `many` cardinality. The accepted access subset is limited to
  primary-key lookups, declared-index prefix/range scans, bounded ordered
  unions, and bounded same-partition equijoins through a complete key or
  declared index. Every per-parent and whole-query row/fan-out bound MUST be
  statically proven.
- `RQL-003`: Resolution and planning MUST use contract symbols while emitting a
  closed canonical internal access program. Planning MUST be deterministic,
  MUST prove one partition route and index-compatible ordering, and MUST expose
  stable plan identity and a bounded explain representation.
- `RQL-004`: Required entity, field, index, and row-scope access MUST be derived
  from the complete query before execution and authorized through the shared
  service. Unauthorized requested data MUST reject the query rather than be
  silently omitted unless the declared result schema explicitly models
  redaction.
- `RQL-005`: Parser, resolver, type, locality, bound, index, cardinality, and
  authorization failures MUST produce bounded redaction-safe diagnostics with
  stable codes, contract/query symbols, source spans, and safe remediation.
  A missing usable index SHOULD identify the smallest compatible contract-index
  change without revealing inaccessible schema.
- `RQL-006`: A field from an earlier explicitly bounded `many` binding MAY be
  consumed only as a bounded collection by `in` to supply one component of a
  later `many` binding's complete primary key. The remaining key components,
  same-partition route, compatible source/target types, canonical order,
  target bound no greater than the source bound, and a declared missing-target
  outcome MUST be statically proven. Collection-as-scalar use, singular-as-set
  use, non-key fan-out, Cartesian products, and unbounded nested access MUST be
  rejected with source-spanned diagnostics.

- `QRY-001`: One query request MUST execute its complete closed access program
  against one authoritative storage snapshot and return only owned bounded
  observations after the storage transaction closes. No engine iterator,
  callback, or transaction handle may cross the query-execution port.
- `QRY-002`: Pagination MUST be snapshot-per-request. A cursor MUST bind the
  principal, exact contract/module/plan identities, canonical parameter hash,
  ordering and continuation state, authorization constraint, and observed
  index epochs; any incompatible or stale cursor MUST fail closed.
- `QRY-003`: A deployed query module MUST be immutable, content-addressed,
  versioned, and pinned to an exact retained contract lineage, version, and
  bundle hash. Activation MUST be audited and compare-and-swap safe, and
  generated clients MUST pin exact contract and module identities.
- `QRY-004`: Ad-hoc and named query requests and responses MUST use
  name-addressed typed values and generated parameter/result schemas. The
  public application surface MUST NOT require numeric entity, field, index, or
  command-input IDs, encoded keys, protobuf field maps, field masks, or access
  plans from its caller.
- `QRY-005`: A compiled dependent key batch MUST execute every ordered bounded
  target point read inside the query's one authoritative snapshot, preserve
  source order, and validate exact target completeness before release. Empty
  input MUST return an empty collection; null, duplicate, noncanonical,
  over-bound, or malformed keys MUST fail closed; and a missing target MUST
  select the query's declared outcome without partial results.

- `DX-001`: The implementation MUST add an additive versioned application gRPC
  API over the same API-neutral service, authorization, compiler, query
  executor, command runtime, and commit coordinator. Existing `riffdb.v1`
  requests and compatibility fixtures MUST remain supported as the kernel
  protocol.
- `DX-002`: CLI and MCP MUST expose contract description, query check/explain/
  execute, named-query execution, and symbolic command execution through that
  shared service. Read and mutation operations MUST remain distinct in schemas,
  authorization, audit, and MCP risk presentation.
- `DX-003`: Exact named query modules and compiled commands MUST optionally
  generate reproducible Rust and TypeScript parameter/result/operation clients.
  Code generation is a convenience and MUST NOT be required for ad-hoc RiffQL.
- `DX-004`: `riffdb dev` MUST provide bounded local bootstrap, ordinary
  capability grants from named development-role presets, contract/query watch
  mode, and bounded concurrent seed-command execution without bypassing
  production authorization or command semantics.
- `DX-005`: The TicketDesk acceptance application MUST implement every list or
  detail page with one symbolic query request and every mutation with one
  symbolic command invocation, with no caller-visible stable numeric IDs,
  hand-authored protobuf records, key decoding, manual field masks, or public
  N+1 entity requests.
- `DX-006`: TicketDesk's named detail query MUST return the ticket, project,
  organization, optional assignee, bounded comments, and bounded labels reached
  through `TicketLabel` in one symbolic request and one snapshot. Its generated
  result types, demo verification, and benchmark MUST exercise that complete
  shape without a second label query or denormalized label names.

- `SAFE-001`: A stable application capability MUST authorize only exact named
  compiled commands and exact named deployed queries. Named query authority
  MUST bind contract lineage, query-module hash, and query name. It MUST NOT
  authorize ad-hoc query source or a substituted module, query, plan, contract,
  partition, principal, or capability revision.
- `SAFE-002`: Application-query authorization and kernel `ReadEntity`/
  `ScanIndex` authorization MUST use disjoint permission types. Policy MUST
  produce a private exact-plan query proof that cannot be reused, serialized,
  retained in a cursor, or converted into a public kernel request. Existing
  kernel permissions MUST NOT authorize RiffQL, and application permissions
  MUST NOT authorize kernel RPCs.
- `SAFE-003`: Ad-hoc RiffQL check, explain, and execute MUST be separate
  explicit agent/development permissions. `ReadContract`, named-query
  authority, generated-client availability, or tool discovery MUST NOT imply
  ad-hoc execution authority.
- `SAFE-004`: Contracts MUST support declared required same-partition
  relationships whose source components map to one complete target primary
  key. A command that establishes or changes such a relationship MUST fail
  compilation unless a dominating exact target read with a declared missing-
  target outcome is visible in the command plan and revalidated at commit.
  Cross-partition, hidden, partial-key, or implicit-name relationships MUST be
  rejected.
- `SAFE-005`: Contracts MUST support declared same-partition unique keys.
  Every command that can establish or change a declared unique value MUST
  acquire the compiler-derived input-computable unique conflict capability,
  validate the exact unique key, and update the authoritative unique index
  atomically with the entity. Global, unbounded, collation-dependent, or
  undeclared uniqueness MUST NOT be inferred.
- `SAFE-006`: Every compiled query MUST contain one canonical whole-request
  cost vector covered by its plan hash. Policy MUST authorize the complete
  vector once, and execution MUST consume matching fuel for steps, scans,
  point reads, dependent keys, intermediates, projected values, and encoded
  output. Repeating individually bounded steps MUST NOT amplify the admitted
  budget, and exhaustion MUST release no partial result or cursor.
- `SAFE-007`: Stable generated SDKs, ordinary application capability presets,
  and normal application MCP catalogs MUST expose only named operations.
  Kernel operations and ad-hoc source construction MUST require visibly
  separate profiles and credentials; there MUST NOT be a default preset that
  silently combines stable application, agent, and kernel authority.
- `SAFE-008`: The application milestone MUST include negative programs proving
  rejection of generic writes, raw kernel reads under application authority,
  ad-hoc source under named-only authority, module/query/plan substitution,
  dangling declared relationships, concurrent declared-unique collisions,
  cumulative query-cost amplification, public N+1/multi-snapshot page
  composition, and dependent-batch field/partition/cursor/revocation escape.
  Safety claims MUST remain scoped to declared RiffDB semantics and MUST NOT
  imply control over undeclared business rules or external application effects.

- `PERF-001`: The application path MUST publish bounded redaction-safe latency
  decomposition for transport, authentication, current-view lookup,
  authorization, catalog/query compilation or cache lookup, authoritative
  storage execution, and response encoding. Optimization evidence MUST compare
  equivalent semantics over a reused HTTP/2 connection.
- `PERF-002`: On the checked reference-machine profile, warm p50 latency MUST
  be at most 5 ms for point reads, 15 ms for representative list/detail
  queries, and 20 ms for one command; TicketDesk's 276-row seed MUST complete
  within 3 seconds. The measured incremental unary gRPC transport/adaptation
  cost MUST be at most 1 ms. A missed gate blocks WP-270 rather than weakening
  authorization, durability, snapshot, command, audit, or compatibility
  semantics.
- `PERF-003`: The release-derived application benchmark MUST publish command
  throughput and commit-duration samples across increasing retained database
  sizes under the exact configured acknowledgement durability. The final
  measured window MUST retain at least 50 percent of the first steady-state
  window's throughput and MUST NOT fall below 50 committed commands per second
  on the checked reference-machine profile. A miss blocks Agent Application
  Alpha. It MUST NOT be repaired by silently reducing durability, disabling a
  reviewed crash defense, weakening atomic records, or acknowledging before
  the configured durable boundary.
- `PERF-004`: Production online writes MUST pass through one bounded typed FIFO
  scheduler. It MAY group at most 64 compatible transitions for at most 200
  microseconds and MUST retain the existing 64-command and 16 MiB transaction
  ceilings. The public command transport batch MUST remain capped at 16
  ordinary commands. A newly executed, successfully returned application
  command MUST reach one atomic durable terminal boundary containing its
  mutation graph, outcome, provenance, event intent, commit identity, and
  linked terminal audit. Every item MUST retain independent identity, sequence,
  outcome, provenance, audit, acknowledgement, and uncertainty recovery. The
  storage mechanism MAY use one or more private physical phases but MUST NOT
  release a response or observable application state before the complete
  durable boundary. On the checked profile, a saturated grouped workload MUST
  deliver at least twice the hardened synchronous oracle's throughput without
  a p99 scheduler wait above 2 ms or starvation. A miss blocks Agent
  Application Alpha and MUST NOT be repaired by weaker durability, audit,
  authorization, atomicity, visibility, or response-release semantics.
- `PERF-005`: A command MAY observe entities owned by multiple aggregates only
  when the compiler proves that every binding is in one identical partition.
  Every `create` and `mutate` binding MUST remain in exactly one mutation
  aggregate, and only that aggregate derives logical conflict keys. External
  observations MUST remain explicit and MUST be revalidated exactly inside the
  authoritative commit transaction; cross-partition reads and multi-aggregate
  writes MUST be rejected. Server evidence MUST report the bounded effective
  completion-group-size distribution without high-cardinality labels. On the
  checked profile, the full public-command TicketDesk seed and representative
  unary mutation p50 MUST each complete within twice the same-run PostgreSQL
  result under the same semantic acknowledgement durability and audit
  semantics. A
  miss blocks Agent Application Alpha and MUST NOT be waived through direct
  storage/import writes or weaker safety.
- `PERF-006`: After receiving the oldest groupable transition, the production
  scheduler MAY wait for compatible work until that transition's existing
  200-microsecond deadline even when an intermediate queue poll is empty. It
  MUST drain into a bounded ordered buffer, select at most 64 compatible
  commands, preserve every deferred message's relative order, and treat
  capability, catalog, administrative-write, shutdown, fencing, and readiness
  transitions as non-bypassable barriers. Historical idempotency selection MAY
  use one bounded read transaction for up to 16 public-batch items, but
  authoritative admission MUST recheck every identity and canonical input
  inside the write transaction. Replay audit transitions MUST participate in
  bounded groups. Coordinator admission MUST have independent count and byte
  ceilings above one maximum command group, while the public batch remains
  capped at 16.
- `PERF-007`: One completed production activation MUST release exactly one
  authoritative writer and MAY release cloneable least-authority MVCC reader
  handles. Admission and completion commits MUST NOT hold a process-wide
  storage mutex across redb transaction work or durable flush, and operational
  reads MUST NOT share such a mutex. The no-destination outbox composition MAY
  derive post-start readiness from an exact commit-owned monotonic latch seeded
  by recovery, but MUST NOT report ready after any undelivered intent. Named
  RiffQL parsing MAY be cached only under complete contract, module, and query
  identities. Concurrent read/write evidence MUST prove snapshot consistency,
  bounded admission, shutdown, poisoning, and fail-closed activation.
- `PERF-008`: Checked command plans, schemas, bundles, and query programs MAY
  use immutable shared ownership under complete content identities. Repeated
  lookup or normalization MAY be replaced by exact identity revalidation, but
  fresh authorization and response-release safe points remain mandatory.
  Durable sizing MAY avoid materializing sizing-only Protobuf graphs only when
  its structural upper bound dominates every final encoding and compatibility
  fixtures remain byte-identical. Already-admitted commands MAY perform
  compiler-declared snapshot reads and deterministic preparation in a bounded
  worker pool, but conflict ownership and final commit sequencing MUST remain
  admission ordered, transaction-current revalidation MUST remain inside the
  sole writer transaction, and no worker may perform an untracked effect. On
  the checked profile, the full public TicketDesk seed and every representative
  unary scenario MUST be within 1.10 times same-run PostgreSQL; at 32 clients,
  public mixed-workload throughput MUST be at least 0.90 times PostgreSQL and
  p95 latency MUST be at most 1.25 times PostgreSQL. A miss blocks WP-370.
- `PERF-009`: The standard application durability profile MUST acknowledge only
  after a redb Immediate one-phase checksummed commit is known successful. A
  hardened two-phase profile and recovery oracle MUST remain available. Both
  profiles expose identical atomic command, idempotency, audit, provenance,
  event, outbox, and uncertainty semantics. Configuration MUST state the
  non-Byzantine host/storage assumption, and crash/reopen evidence MUST prove
  complete-terminal-or-absent recovery for the standard profile.
- `PERF-010`: A synchronous mutation MAY perform bounded side-effect-free
  preparation before durable admission, but after preparation it MUST freshly
  authorize and atomically transition a vacant equal identity directly to its
  complete terminal command graph and linked `Started`/terminal audit. It MUST
  expose no output or effect before that commit. Concurrent same-identity
  attempts MUST produce exactly one terminal winner; unknown status MUST fence
  and resolve by identity. Existing durable Pending rows remain exactly
  resumable, while new synchronous commands MUST NOT create Pending merely to
  cross deterministic evaluation.
- `PERF-011`: Storage format V2 MUST use a compact versioned record header with
  a closed record tag, nonzero schema revision, payload checksum, and a
  database-level registry digest. It MUST support mixed revisions during one
  bounded restartable V1-to-V2 migration. The event table MUST own the sole
  complete event payload; commits and outbox state reference exact event IDs and
  hashes created in the same transaction. Unknown framing, registry, revision,
  checksum, key/value, or reciprocal-reference state fails startup closed.
- `PERF-012`: Mutation-affected index invalidation MUST advance at most one
  durable generation for each distinct compiler-proven `(partition, index)`
  pair in a command. Every range dependency and stable cursor MUST bind that
  pair and generation, so any same-partition index mutation conservatively
  invalidates it. The compiler MUST reject a write-influencing range whose
  bounded generation cannot be proved. No application path may select a weaker
  invalidation granularity, and no non-durable command state may become visible
  to reads or derived workers.

The milestone is complete only when WP-205 through WP-300 pass their package
acceptance commands and an independent fresh-agent TicketDesk run satisfies
`DX-005`, `DX-006`, every `SAFE-*` requirement, and the published performance
gates.

## 24.4 Agent Application Alpha milestone

This milestone begins only after the safe symbolic application platform is
complete. It is governed by accepted ADR-0056.
`docs/agent-application-alpha.md` defines the concise gate and measurement
contract.

- `AAA-001`: Generated Rust and TypeScript stable-application clients MUST own
  typed parameter serialization, exact named invocation, typed result and
  declared-outcome decoding, cursor handling, public-error decoding, and
  read-after-commit options. A first-party acceptance application MUST contain
  no handwritten RiffDB parameter map, encoder, decoder, transport-status
  parser, or RPC wrapper.
- `AAA-002`: Generated command methods MUST preserve one caller-supplied or
  configured durable idempotency identity across retry and uncertain-outcome
  recovery. Generated clients MUST pin and verify exact contract and query-
  module identities and MUST fail closed rather than silently negotiate a
  different operation shape.
- `AAA-003`: One versioned symbolic application manifest MUST bind contract
  source, named query modules, roles, generation targets, and seed inputs.
  Symbolic roles MUST name only application operations and tenant/environment
  scope; the compiler MUST derive contract-description, command, private query,
  field/index/partition/cost, result-visibility, lineage, and MCP visibility
  requirements without exposing numeric IDs, masks, raw capability bits, or
  reusable kernel permissions.
- `AAA-004`: Every public application failure MUST use one bounded versioned
  semantic error envelope with stable code/category/recovery action, attempted
  operation, authorized contract/module context, authorized symbolic path and
  caller-source span when applicable, a static safe message, closed suggested-
  fix codes, and a trace identifier when available. A failure with no valid
  symbol path MUST NOT fabricate one.
- `AAA-005`: Application errors MUST be authorization-filtered and redacted
  before serialization and MUST contain no submitted free-form values,
  credentials, hidden schema, arbitrary server prose, or internal source.
  gRPC, Rust, TypeScript, CLI, and MCP MUST preserve the same machine-readable
  semantics and retry/recovery classification.
- `AAA-006`: Bulk seed/import MUST execute every item as an ordinary exact
  named compiled command with its own canonical input, idempotency identity,
  authorization, typed outcome or structured error, provenance, and commit.
  A batch MUST NOT expose generic writes or claim collection-wide atomicity.
- `AAA-007`: Command batches MUST enforce bounded concurrency, streaming
  backpressure, input/output limits, cancellation, progress, and resumable
  same-key checkpoints. Every item MUST correlate with one bounded import/seed
  session, and retry MUST NOT duplicate a committed command.
- `AAA-008`: `riffdb new <application>` MUST create the canonical application
  manifest, contract/query/role/seed layout and generated-output roots.
  `riffdb dev` MUST start local RiffDB, compile/deploy, bind the selected role,
  generate Rust/TypeScript/MCP artifacts, run the command-batch seed, and watch
  accepted inputs without weakening production service or authorization
  semantics.
- `AAA-009`: Normal Rust/TypeScript application facades, templates,
  quickstarts, generated clients, and MCP catalogs MUST exclude kernel
  requests by default. Kernel/admin use MUST require an explicit package or
  unstable/administrative feature plus separate credential. A mandatory
  boundary linter MUST reject kernel imports and handwritten transport glue in
  first-party acceptance applications.
- `AAA-010`: Rust and TypeScript MUST consume the same application manifest,
  operation schemas, compatibility fixtures, and golden observations and MUST
  provide equivalent named queries, commands, outcomes, optional/nested
  results, pagination, read-after-commit, error recovery, development
  credentials, hot reload, and boundary checks. TypeScript acceptance MUST run
  a real web application, not only type-check generated source.
- `AAA-011`: Blog/CMS and orders/inventory application corpora MUST be attempted
  before another RiffQL construct is accepted. Every unsupported shape MUST
  retain a source-spanned diagnostic fixture classifying the locality, bound,
  index, authorization, cardinality, or language cause and suggesting an
  index, relationship, projection, or decomposition when sufficient. Any new
  grammar/IR construct MUST stop for a separate accepted ADR and MUST be the
  smallest bounded construct supported by repeated application evidence.
- `AAA-012`: The Agent Application Alpha gate MUST run at least four fresh
  independent sealed evaluations covering both domains in both Rust and
  TypeScript without RiffDB implementation or TicketDesk source. Every run
  MUST publish interventions, kernel attempts, handwritten glue lines,
  compiler/runtime failures, unsupported shapes, time to first write/read and
  completion, source access, and rating. Every run MUST meet the thresholds in
  SPEC Section 20.4.2; failures MUST reopen the owning package rather than be
  patched in the evaluator.
- `AAA-013`: Application authors MUST declare only symbolic source, role,
  generation-target, and seed intent. The compiler MUST produce one
  deterministic exact lock covering source, contract, module, query, plan,
  schema, role-authority, compiler-format, and generated-artifact identities.
  Generation, role binding, and non-development deployment MUST reject missing,
  stale, partial, or mismatched locks. No lock-writing operation may deploy,
  grant, bind, invoke, or silently widen authority.
- `AAA-014`: Contract, query, planner, manifest, role, lock, generation, and
  scaffold failures MUST retain one bounded machine-readable authoring
  diagnostic with stable stage/code, source span when available, symbolic
  path, closed cause/fix codes, file-change disposition, and retry
  classification. Diagnostics MUST NOT contain credentials, runtime values,
  hidden schema, arbitrary engine prose, or internal sources.
- `AAA-015`: `riffdb new` MUST support a new child or an existing empty regular
  writable directory without overwriting or following a destination symlink.
  The public bundle MUST contain the complete versioned contract-language
  reference, command/invariant cookbook, machine-readable application schema,
  inspection workflow, domain-neutral examples, and negative diagnostic
  examples needed to replace the scaffold through public inputs alone.
- `AAA-016`: The generated TypeScript repository MUST be a complete
  offline-buildable server-side web application with exact product runtime and
  toolchain inputs. Builder MCP MUST expose the same bounded local describe,
  check, diagnostic, lock-preview, explicit lock-write, and generation
  semantics as CLI while possessing no storage, deployment, role-binding,
  execution, or credential authority.
- `AAA-017`: Before an official rerun, one sealed public-only rehearsal MUST
  complete Blog and Orders in Rust and TypeScript and exercise invalid source,
  stale lock, missing index, unsafe role, interrupted generation, and the
  growing-database performance gate. Two fresh canary agents MUST then
  complete Blog/Rust and Orders/TypeScript with every AAA-012 threshold,
  including ratings of at least 8.5.
- `AAA-018`: Evaluation evidence MUST retain failed campaigns immutably and
  select one exact bundle/campaign. First-write and first-page metrics count
  only generated RiffDB operations whose returned contract, module, query,
  plan, and commit identities match the golden workload. An unbacked
  application response or harness self-test MUST NOT satisfy an agent metric.

The milestone is complete only when WP-305 through WP-370 pass, ADR-0056,
ADR-0057, ADR-0058, and every language or public/durable interface decision required by
those packages is accepted, and the raw sealed evaluation reports are
published.

---

# Appendix A. Core state and storage key examples

These examples illustrate ordering and namespace separation. The exact byte encoding is owned by `riffdb-storage-api` and versioned.

```text
meta/format_version
meta/database_id
meta/next_application_sequence
meta/next_administration_sequence
contract_bundles/<u32-be-lineage-length>/<lineage-utf8>/<u64-be-version>
catalog_active/0x01
entities/<exact-canonical-entity-key>
secondary_indexes/<exact-canonical-index-entry-key>
index_generations/<u32-be-partition-key-length>/<canonical-partition-key>/<u32-be-index-id>
idempotency/<database-id>/<environment>/<tenant-scope>/<principal-id>/<contract-lineage>/<command-id>/<key-digest>  # one full StoredOutcomeV1 or StoredExecutionFailedV1 envelope
idempotency_pending/<canonical-idempotency-identity>  # deleted atomically at terminalization; no tombstone
commits/<u64-be-sequence>
provenance/<provenance-id>
events/<u64-be-commit-sequence>/<u32-be-event-ordinal>
outbox/<event-id>
outbox_status/<event-id>
projection_frontier/0x46-0x01/<lineage>/<projection-id>/<projection-plan-hash>
projection_state/0x47-0x01/<projection-identity>/<nonzero-generation>/<framed-group-components>
projection_applied/0x41-0x01/<projection-identity>/<nonzero-generation>/<nonzero-commit-sequence>
capabilities/0x01/<capability-id>
capability_tokens/0x01/<digest-scheme>/<digest-key-id>/<32-byte-digest>
meta/capability_bootstrap/v1
audit/0x01/<nonzero-administration-sequence>
```

Keys MUST not embed raw secrets. Lexicographically ordered integer components use big-endian encoding. Every key namespace has a format version or is migrated atomically with the database format.

---

# Appendix B. Public service sketch

```protobuf
syntax = "proto3";
package riffdb.v1;

service ContractService {
  rpc ValidateContract(ValidateContractRequest) returns (ValidateContractResponse);
  rpc ExplainCommand(ExplainCommandRequest) returns (ExplainCommandResponse);
  rpc DeployContract(DeployContractRequest) returns (DeployContractResponse);
  rpc GetActiveContract(GetActiveContractRequest) returns (GetActiveContractResponse);
  rpc GetContractVersion(GetContractVersionRequest) returns (GetContractVersionResponse);
  rpc DiscoverCommandTools(DiscoverCommandToolsRequest) returns (DiscoverCommandToolsResponse);
  rpc DiscoverResources(DiscoverResourcesRequest) returns (DiscoverResourcesResponse);
}

service CommandService {
  rpc Execute(ExecuteCommandRequest) returns (ExecuteCommandResponse);
  rpc GetOutcome(GetOutcomeRequest) returns (GetOutcomeResponse);
}

service QueryService {
  rpc GetEntity(GetEntityRequest) returns (GetEntityResponse);
  rpc ScanIndex(ScanIndexRequest) returns (ScanIndexResponse);
  rpc QueryProjection(QueryProjectionRequest) returns (QueryProjectionResponse);
  rpc GetProjectionStatus(GetProjectionStatusRequest) returns (GetProjectionStatusResponse);
}

service CommitService {
  rpc GetCommit(GetCommitRequest) returns (GetCommitResponse);
  rpc ScanCommits(ScanCommitsRequest) returns (ScanCommitsResponse);
  rpc SubscribeCommits(SubscribeCommitsRequest) returns (stream CommitNotification);
  rpc TraceProvenance(TraceProvenanceRequest) returns (TraceProvenanceResponse);
}

service AdminService {
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc Stats(StatsRequest) returns (StatsResponse);
  rpc CreateCapability(CreateCapabilityRequest) returns (CreateCapabilityResponse);
  rpc RevokeCapability(RevokeCapabilityRequest) returns (RevokeCapabilityResponse);
  rpc ListPendingOutboxDeliveries(ListPendingOutboxDeliveriesRequest) returns (ListPendingOutboxDeliveriesResponse);
  rpc CreateOfflineBackup(CreateOfflineBackupRequest) returns (CreateOfflineBackupResponse);
  rpc RestoreOfflineBackup(RestoreOfflineBackupRequest) returns (RestoreOfflineBackupResponse);
  rpc GetOfflineMaintenanceOperation(GetOfflineMaintenanceOperationRequest) returns (GetOfflineMaintenanceOperationResponse);
}
```

This appendix intentionally repeats the five canonical kernel services and exact
25-RPC `riffdb.v1` inventory from Section 11.2. That inventory remains
compatible in v0.36; ADR-0052's additive `riffdb.app.v1` application service is
versioned and tested separately. Public messages MUST use stable field numbers, reserve removed
fields, bound nested sizes, use the exact `Value` family in Section 11.3, and
distinguish absent values from defaults when semantics require it.

---

# Appendix C. Error taxonomy

| Layer | Example | Public representation |
|---|---|---|
| Declared business outcome | `InsufficientBudget` | Normal typed outcome; not transport error |
| Input/schema error | Invalid UUID or unknown field | Validation error with bounded field diagnostics |
| Idempotency misuse | Same key, different canonical input | Stable conflict error containing no previous sensitive input |
| Authorization | Command or field not allowed | Permission denied; tool may also be hidden from catalog |
| Concurrency admission | Lock deadline or fixed coordinator reevaluation budget exhausted | Retryable `ConcurrencyDeadlineExceeded` with safe retry guidance |
| Contract mismatch | Client invokes command absent from active bundle | Failed precondition with active version and refresh hint |
| Storage unavailable | Commit cannot be made durable | Unavailable/internal class; no success claim |
| Outcome uncertainty | Client lost response after submission | Client resolves through same idempotency key or `GetOutcome` |
| Deterministic command execution failure | Dependency-validated arithmetic or fixed resource-limit fault | `CommandExecutionFailed` with closed code and `CONTACT_OPERATOR`; no application commit or sequence |
| Projection lag | Required sequence not reached by deadline | Typed projection `WaitTimedOut` result |
| Projection degraded | Worker cannot advance | Typed degraded result with safe reason |
| Maintenance unavailable | Server is draining/offline/validating or receipt recovery is incomplete | Typed unavailable/failed-closed class; retry the same maintenance operation ID after service recovery |
| Destructive confirmation absent | Restore targets a nonempty directory without `ALLOW_REPLACE_NONEMPTY_TARGET` | Caller-correction error; no drain or publication |
| Internal bug | Runtime invariant or impossible state | Opaque incident ID, server-side diagnostics, fail closed |

Errors MUST make it possible to distinguish safe retry, same-key outcome resolution, caller correction, permission escalation request, and operator intervention.

---

# Appendix D. Source and dependency references

The implementation MUST prefer primary project documentation and pin reviewed versions in the workspace rather than treating the versions below as unbounded ranges.

1. Agentic OLTP Database — Concept Design, Draft, July 2026.
2. Model Context Protocol specification, version 2025-11-25: architecture, lifecycle, tools, resources, transports, authorization, cancellation, and progress.
3. Official Model Context Protocol Rust SDK (`rmcp`) exactly 2.2.0, with the
   ADR-0008 feature, source, registry-fallback, framing/logging, build-script,
   native, unsafe, cryptographic, license, advisory, and lock-graph review. Any
   different real resolution requires renewed human review before merge.
4. Rust release channel and Rust 1.97.0 release notes.
5. Tokio asynchronous runtime documentation.
6. Tonic gRPC implementation documentation.
7. Prost Protocol Buffers implementation documentation.
8. Protox pure-Rust Protocol Buffers compiler documentation.
9. redb embedded database documentation.
10. Fjall embedded LSM storage documentation.
11. Logos lexer documentation.
12. LALRPOP parser generator documentation.
13. Miette diagnostic framework documentation.
14. tracing structured diagnostics documentation.
15. Proptest, Loom, Shuttle, cargo-fuzz, and Insta documentation.
16. OpenRaft documentation for the replicated beta research and implementation stage.

---

# Appendix E. Requirement index

| Prefix | Domain |
|---|---|
| `POC-*` | POC success and acceptance |
| `SYS-*` | System boundary and implementation policy |
| `ID-*` | Stable identifier semantics |
| `VAL-*` | Canonical transactional values |
| `ENT-*` | Authoritative entity records |
| `CMP-*` | Contract compiler and language |
| `DSL-*` | Contract language restrictions |
| `OUT-*` | Typed outcome and idempotency behavior |
| `TXN-*` | Command execution, conflict ownership, and validation |
| `LOG-*` | Commit sequence, log, and provenance |
| `STO-*` | Durable storage and compatibility |
| `REC-*` | Restart and recovery behavior |
| `API-*` | Shared service and public API |
| `MCP-*` | Model Context Protocol interface |
| `SEC-*` | Authorization, capabilities, and data handling |
| `EFF-*` | Durable effect and outbox semantics |
| `PRJ-*` | Projection semantics |
| `TEST-*` | Verification and failpoint requirements |
| `REP-*` | Replication and distribution boundary |
| `RQL-*` | RiffQL syntax, semantics, planning, and diagnostics |
| `QRY-*` | Composite query execution and immutable query modules |
| `DX-*` | Symbolic application surfaces, generation, and local workflow |
| `SAFE-*` | Safety-by-construction application authority, declared integrity, budgets, and negative acceptance |
| `PERF-*` | Application-path measurement and performance gates |
| `AAA-*` | Agent Application Alpha bindings, roles, diagnostics, batches, scaffolding, language parity, and evaluation |

Every normative requirement MUST be traceable to at least one automated test, review checklist item, or explicitly justified manual verification artifact before its stage can pass.
