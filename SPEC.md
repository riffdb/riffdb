# RiffDB
## Standalone Rust POC Technical Specification

### Native MCP interface and roadmap to MVP

**Tagline:** *Vibe fast. Commit safely.*  
**Category:** Contract-first operational database for agent-built applications  

**Version:** 0.13
**Status:** Architecture-approved implementation handoff
**Date:** 20 July 2026
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
| Primary application API | gRPC over HTTP/2 |
| Native agent API | Model Context Protocol (MCP) |
| Storage baseline | `redb`, behind a narrow internal storage interface |
| Rust baseline | Rust 1.97.0 |
| MCP baseline | MCP specification 2025-11-25 |
| MCP Rust SDK baseline | `rmcp` 2.2.x |

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

### Normative language

The terms **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative. Requirement identifiers such as `TXN-004` are stable references for issues, pull requests, tests, and agent work packages.

### Scope labels

- **POC**: Required to demonstrate the core contract-first thesis on a durable single node.
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

1. Network input is untrusted until transport decoding, size checks, authentication, and schema validation complete.
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
      riffdb-grpc/           # canonical public SDK/gRPC adapter added after WP-130
  scripts/
  docs/
```

The binary targets are `riffdbd` from `riffdb-server`, `riffdb` from `riffdb-cli`, and `riffdb-mcp` from `riffdb-mcp-stdio`.

## 5.2 Crate responsibilities

| Crate | Public responsibility | Allowed dependency direction |
|---|---|---|
| `riffdb-types` | Stable identifiers, pure checked UUIDv7 assembly, canonical value types, versions, timestamps, decimals, and common value semantics | Foundation crate; no clock, entropy, storage, or transport dependencies |
| `riffdb-errors` | Public-safe errors, internal error layering, redaction boundaries, incident identifiers, and the consumer-owned `IncidentIdSource` port | Types only; no transport-specific errors, clock, entropy, or concrete incident provider in core layers |
| `riffdb-proto` | Generated public/durable Protobuf types, envelopes, descriptors, wire validation, and foundational value/error conversion helpers | Types, errors, and Prost only; no storage API, runtime, service, or transport dependency |
| `riffdb-contract-syntax` | Lexer, parser, source spans, syntax AST, and parser diagnostics | Types and parser tooling only |
| `riffdb-contract-ir` | Typed HIR, executable IR, plans, immutable projection group schemas, and contract bundle structs | Types; no parser implementation, storage, compiler implementation, or runtime dependency |
| `riffdb-contract-compiler` | Name resolution, type checking, invariant classification, locality analysis, plan generation, and JSON Schema output | Syntax and IR; no storage, service, or transport dependency |
| `riffdb-catalog` | Immutable contract bundle persistence, compatibility checks, active-version changes, catalog notifications, and IR-aware validation of bounded same-session historical startup evidence | IR and storage API; its opaque `ValidatedCatalogHistory` never crosses a storage trait |
| `riffdb-storage-api` | Semantic snapshots, database initialization, structural startup sessions/evidence/type-state ports, evaluated commands, commit intents, durable DTOs, typed transitions/readers, the narrow durable `proto_codec` mapping bridge, and ADR-0017 projection-schema consumers | Types/errors; proto only through `proto_codec`; contract IR only for immutable `ProjectionGroupSchema`/`BoundProjectionGroupSchema` values in the projection-schema module; no clock, entropy, compiler, command-plan interpretation, historical-plan validation, runtime, commit, service, transport, or concrete-engine dependency |
| `riffdb-storage-memory` | Deterministic reference storage implementation used by model and semantic tests | Storage API only |
| `riffdb-storage-redb` | Durable POC implementation, table layout, complete structural integrity/evidence scan, dormant opened ports, backup, restore, and engine benchmarks | Storage API and `redb`; no contract IR, `ValidatedCatalogHistory`, readiness composition, or API transports |
| `riffdb-invariant` | Shared pure evaluation of checked input-computable expressions plus supported predicates, invariants, postconditions, and commit-time checks | Types and IR; no snapshot, admission, storage, service, clock, entropy, or transport dependency; no POC state-machine source, IR, or execution surface |
| `riffdb-runtime` | Deterministic command-plan interpreter that consumes owned snapshots and produces `EvaluatedCommand` without external I/O | IR, invariant engine, and storage semantic value/snapshot types; no provenance claims, admission persistence, storage engine, service, or transport dependency |
| `riffdb-conflict` | Canonical conflict keys, exclusive logical capabilities, wait queues, cancellation, and hot-key diagnostics | Types and synchronization primitives; no storage engine |
| `riffdb-idempotency` | Canonical command identity, input hashing, persisted outcome lookup, duplicate detection, and uncertain-result recovery | Types and storage API |
| `riffdb-commit` | Database-initialization executor, admission, capability acquisition, deterministic evaluation orchestration, final `CommitIntent` assembly, revalidation, sequencing, authoritative commit, typed control-plane operations, ordered audit execution, and consumer-owned `AdmissionClock`, `AdministrationClock`, and `ProvenanceIdSource` ports | Runtime, conflict, idempotency, catalog, storage API, and policy-owned authorized-preparation/provenance/facts values plus only `TransactionCurrentCapabilityVerifier` and `AuthorizationClock`; no service, transport, protocol, general policy authorizer, obligations/redaction engine, policy-owned storage reader, or concrete clock/entropy implementation |
| `riffdb-auth` | Principal authentication, local development capability tokens, expiry, credential resolution, narrow synchronous authentication clock, and `CredentialAuthenticator` entry point | Types, errors, and storage-owned capability readers; no policy, command execution, service, or transport dependency |
| `riffdb-policy` | Deny-by-default authorization, capability scopes, obligations, approvals, provenance validation, value-only authorized capability-mutation preparation and transaction-current facts, synchronous authorization clock, and pure transaction-current capability verification | Auth and types/errors only; no storage API, commit, service, transport, protocol, authoritative write handle, or concrete storage dependency |
| `riffdb-service` | API-neutral command, contract, entity, commit, provenance, projection, discovery, administration, and health services, including capability-administration request/result semantics and checked command-input preparation | Foundational types/errors, contract/compiler/catalog semantics, the pure `riffdb-invariant` expression evaluator, auth and policy entry points, typed commit executors, and consumer-owned bounded read ports; no runtime execution API, transport, general storage engine, or concrete storage implementation |
| `riffdb-api-grpc` | Tonic services, authentication interceptors, bounds checks, and wire conversions | Service, the auth-owned `CredentialAuthenticator` interface, proto, and Tonic; no policy, catalog, runtime, commit, storage API, or storage implementation |
| `riffdb-client-rust` | Generic and generated Rust client APIs plus the approved system UUIDv7 request/capability/agent-session convenience source | Proto and Tonic client plus the exact ADR-0018 entropy dependency only; no semantic database implementation |
| `riffdb-server` | `riffdbd` process composition, configuration, lifecycle, hosted gRPC/HTTP endpoints, startup proof composition, and concrete OS clock/UUIDv7/cursor/digest providers implementing the separate consumer ports | Service/API crates and concrete auth, clock/identifier, executor, storage, outbox, projection, and observability implementations solely for composition; no new policy, command, query, redaction, cursor, audit, or identifier semantics |
| `riffdb-api-mcp` | MCP tool/resource catalogs, schema translation, authorization-aware discovery presentation, Streamable HTTP authentication/handling, and protocol adaptation | Service, the auth-owned `CredentialAuthenticator` interface, `rmcp`, and JSON Schema support; no direct policy, catalog, runtime, commit, storage API, or storage implementation |
| `riffdb-mcp-stdio` | `riffdb-mcp` local stdio bridge that invokes the shared public service | MCP client/server transport glue only; no storage access |
| `riffdb-cli` | `riffdb` operator and developer CLI using public APIs | Rust client and bounded local configuration; may depend only on the isolated pure `riffdb-auth::bootstrap_secret` module for offline bootstrap credential generation/validation and protected-file loading, never on authentication, policy, storage, or service internals |
| `riffdb-outbox` | Durable event dispatch state machine, leases, retries, deduplication metadata, and connectors | Storage API and Tokio; outside command execution |
| `riffdb-projection` | POC event-derived filters, counts, sums, durable frontiers, rebuilds, and read-after-sequence outcomes | Ordered commit scans and storage API |
| `riffdb-observability` | Structured tracing, metrics, health signals, and safe telemetry helpers | Cross-cutting interfaces without business semantics |
| `riffdb-diagnostics` | Bounded explain output, compiler/runtime diagnostic rendering, and safe operator-facing reports | Types, compiler plans, and observability helpers |
| `riffdb-testkit` | Reference model, generated histories, failpoints, temporary servers, recovery harnesses, and shared fixtures | May depend on implementation crates only in test contexts |

## 5.3 Dependency direction rules

- Compiler crates MUST NOT depend on runtime, storage, gRPC, or MCP crates.
- Runtime MUST NOT depend on a concrete storage engine.
- Storage implementations MUST NOT depend on gRPC or MCP.
- API crates MUST NOT access storage implementations directly.
- MCP and gRPC MUST share the `riffdb-service` authorization and execution entry points.
- `riffdb-proto` MUST NOT depend on `riffdb-storage-api`; only the narrowly scoped storage-owned `proto_codec` bridge may map semantic durable DTOs to generated messages.
- `riffdb-storage-api` MAY consume only the two immutable checked ADR-0017 projection schema values from `riffdb-contract-ir`; it MUST NOT consume `CommandPlan` or compiler/runtime services, and `riffdb-contract-ir` MUST NOT depend on storage.
- gRPC and MCP HTTP MAY call only the auth-owned `CredentialAuthenticator` before constructing service request context; production MCP stdio, CLI, and SDK use public gRPC.
- Generated code MUST be checked in only when generation is deterministic and CI verifies it is current.

## 5.4 Baseline libraries

Versions are the verified July 2026 starting point, not a promise to track every release. The lockfile is authoritative.

| Area | Baseline | Use |
|---|---|---|
| Toolchain | Rust 1.97.0 | Workspace toolchain and CI baseline |
| Async runtime | Tokio 1.52.x | Networking, service tasks, channels, timeouts |
| gRPC | Tonic 0.14.x | Public RPC server and client |
| Protobuf | Prost 0.14.x | Wire and durable record generation |
| Pure-Rust proto compiler | Protox 0.9.x | Build without requiring an external `protoc` executable |
| Embedded storage | redb 4.1.0, default features disabled, no optional features | POC durable state and atomic commits; direct only in `riffdb-storage-redb` |
| Backup checksum | sha2 0.11.0, default features disabled | SHA-256 backup manifests; direct only in `riffdb-storage-redb` and never command semantics |
| Storage comparison | Fjall 3.1.x | POC-exit benchmark and possible MVP engine |
| MCP SDK | rmcp 2.2.x | Native MCP server, stdio, Streamable HTTP |
| OS entropy | getrandom 0.3.4, default features disabled, no optional features | Capability/bootstrap identifiers and tokens in `riffdb-auth`, request/capability/agent-session convenience IDs in `riffdb-client-rust`, and injected database/provenance/request/incident/cursor IDs in `riffdb-server`; never command runtime |
| Base64url | base64 0.22.1, default features disabled, `alloc` only | Canonical capability token text in `riffdb-auth` |
| Secret cleanup | zeroize 1.8.1, default features disabled, `alloc` only | Owned auth secret buffers; no claim about transport-generated copies |
| Lexer | Logos 0.16.x | Contract tokenization |
| Parser | LALRPOP 0.23.x | Contract grammar |
| Diagnostics | Miette | Compiler errors with source spans and stable diagnostic codes |
| Tracing | tracing 0.1.x | Structured spans and events |
| Property testing | Proptest 1.x | Generated contracts, values, and histories |
| Exhaustive concurrency | Loom 0.7.x | Small lock-manager and notification components |
| Randomized concurrency | Shuttle 0.9.x | Larger command and task schedules |
| Benchmarks | Dependency-free repeated-run harnesses | Stable local component and scenario benchmarks without adding a POC Criterion dependency |
| Future replication | OpenRaft 0.9.x behind a facade | MVP replicated state machine; not linked into POC server |

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
before the authoritative transaction, never for reads, `ExecutionFailed`, or
terminal replay. Unknown commit status is resolved through idempotency before
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
   retains ADR-0012's dependency-validated terminalization unchanged.
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
   view, and returns canonical read dependencies.
11. The deterministic runtime evaluates the exact checked plan, snapshot,
    immutable `TransactionContext`, and fixed `EvaluationBudget`, with no storage
    transaction, I/O, clock, entropy, provenance claims, or `.await`.
12. A successful mutating evaluation returns storage-owned `EvaluatedCommand`.
    For a new commit attempt, the coordinator obtains one `ProvenanceId` from its
    injected source and combines it with the exact stored admission and plan-
    derived partition/conflict evidence into the final self-contained
    `CommitIntent`. Replay and `ExecutionFailed` generate no provenance ID.
13. An arithmetic or resource fault follows ADR-0012's dependency-validated
    `Pending -> ExecutionFailed` transition; changed evidence triggers full
    reevaluation or leaves the admission pending, and no application sequence is
    assigned.
14. For a commit-required result, the coordinator opens a short synchronous
    write transaction and starts a count-only candidate. Starting the candidate
    checks only the 64-command count ceiling; it does not assign a sequence or
    guess an encoded write-set charge.
15. The transaction rechecks the exact pending identity, input, admission, and
    plan reference, then reads all influential validation targets from
    transaction-current state. The coordinator compares every
    absence/version/epoch dependency and evaluates the exact historical
    commit-check plan over current values plus proposed post-images.
16. After semantic validation, the coordinator derives the canonical set of
    mutation-affected index-prefix epoch targets from transaction-current old
    entries and proposed new entries. Storage reads those epoch positions in the
    same write transaction. They are distinct from influential range-epoch read
    dependencies, except when one exact prefix happens to belong to both sets.
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
20. The coordinator releases logical capabilities and publishes bounded
    post-durability notifications.
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
coordinator-private semantic match; the exact commit-check plan is re-evaluated
over transaction-current values and proposed post-images.

Grammar v1 does not enable write-influencing indexed range reads. A future
bounded indexed command-read IR requires an accepted static-target/epoch policy
and any required exclusion must use explicit conflict keys known before
evaluation. The storage epoch capability and conformance tests do not expose a
hidden runtime scan.

## 9.5 Commit intent

`riffdb-storage-api` owns both `EvaluatedCommand` and the transport-neutral
`CommitIntent`. Runtime constructs only `EvaluatedCommand`, containing the exact
plan reference, canonical binding/range targets and dependencies, complete
mutation post-images with expected observations, ordered pre-commit event
values, and declared encoded outcome. It contains no admission identity, actor,
provenance, logical time, sequence, event ID, durable record, or storage handle.

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

The coordinator performs:

1. Open the durable write transaction and start a count-only candidate.
2. Recheck the exact pending admission and idempotency identity.
3. Read and revalidate every influential transaction-current dependency.
4. Re-evaluate commit-time invariant plans over current values plus proposed mutations.
5. Derive the exact mutation-affected prefix targets from current and proposed index entries.
6. Read every affected epoch position in the same transaction.
7. Freeze the exact sequence-free write plan and reserve semantic plus conservative encoded capacity.
8. Allocate the next commit sequence only after reservation succeeds.
9. Construct and verify the complete entity/index/epoch/outcome/event/outbox/provenance/commit graph against the retained intent, plan, and assignment.
10. Canonically encode every durable envelope and prove its actual per-class byte charge is within the retained upper bound before staging.
11. Stage the complete record graph and commit the storage transaction using configured durability.
12. Publish the committed result to the waiting caller and subscribers only after durable success.

For an ADR-0012 arithmetic or resource fault, the coordinator instead opens the
narrow terminalization transaction, rechecks the complete pending admission and
every influential absence/version/epoch, and atomically records
`ExecutionFailed` only while all evidence is equal. It assigns no application
sequence and constructs none of the command atomic record set. A normal
dependency change writes nothing and follows the bounded full-reevaluation
policy; malformed or missing evidence is an integrity incident and leaves the
admission pending.

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
It owns only structural constructors; the coordinator retains the private proof
that targets, dependencies, mutations, events, and outcome match one exact
checked historical plan.

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
bundle/plan bytes needed by the catalog validator. It also enumerates every
persisted entity key, index-entry key, and range-prefix key as bounded,
IR-opaque evidence with the durable owner/reference facts needed to select its
exact retained or active key schema. Storage checks framing, bounds, canonical
bytes, and record reciprocity but does not interpret a `KeySchema`. The catalog
must consume the exact end of these key-evidence streams and validate every key
against the selected historical schema before it may construct
`ValidatedCatalogHistory`; an unknown owner/schema, incomplete component,
schema mismatch, omitted row, or truncated stream fails closed. This changes no
durable key encoding and introduces no storage-to-IR dependency.

The evidence carries no decoded IR and grants no mutation authority. When the
redb structural scan and every evidence cursor reach exact end, the session may
yield `StructurallyOpened` dormant backend ports bound to that database/open
session. This value proves storage structure, not catalog semantics or
operational readiness. On any failure, the entire open is dropped and no dormant
or operational port is released.

The server drives the evidence pages into the catalog-owned historical validator.
Only server composition may combine matching `StructurallyOpened` ports and
opaque same-session `ValidatedCatalogHistory` with the required readable
digest inventories and lifecycle checks to create operational wiring. The proof
does not pass back through storage, and redb never receives an IR or catalog
dependency. The memory engine implements the same initialization-first,
exclusive-session, exact-end, session-binding, and dormant-port type-state for
conformance tests.

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
500 rows and 4 MiB per generic scan page; 500 records and 16 MiB encoded content
per internal ordered commit-scan page; 4 KiB per
entity/index/partition/conflict key or index prefix; 256 integrity findings plus
a `truncated` flag; 15 MiB per
catalog bundle; 16 targets and 64 KiB per service-audit record; and ADR-0006's
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

`StorageErrorKind` is closed: `Unavailable`, `CommitStatusUnknown`,
`CorruptData`, `IncompatibleFormat`, `LimitExceeded`, `InvariantViolation`, and
`SequenceExhausted`. A backend-proven abort maps to unavailable and never claims
terminal success. Unknown commit status fences further writes until
reopen/integrity recovery and command resolution uses idempotency. Corruption,
incompatibility, impossible transitions, and exhaustion fail readiness with an
opaque incident. Missing records, replay, mismatch, pending admission,
dependency change, catalog conflict, already-revoked capability, projection gap,
and scan end are typed semantic results, never parsed error strings.

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

The `riffdb.storage.v1` compatibility registry contains exactly these 26
top-level `StoredEnvelope` payload types for the POC:

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

The capability and service-audit wire names in that list are fixed even where
the Rust semantic DTO uses a `Stored*` name. Closed helper messages, including
read-dependency collections and capability grants/permission sets, remain nested
and are not separately registered envelopes. No speculative reserved field,
record type, key codec, ADR-0019 deferred metadata, or later-feature placeholder
is part of the v1 registry. Any addition, removal, rename, field-number change,
enum/oneof tag change, or top-level/nested reclassification is a durable-format
change requiring compatibility and recovery review.

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
| `sync` | Commit is acknowledged only after the embedded engine reports durable synchronization. | Default POC correctness mode |
| `group` | Coordinator batches compatible intents and performs one durable flush for the batch. | Semantic interface and benchmark experiments only in the POC |
| `memory` | No durability guarantee. | Unit and model tests only; server refuses non-test startup |

The returned outcome MUST identify the durability mode used for its commit.
The POC production server exposes only `sync` durability. Production `group`
mode remains disabled unless the WP-100 scheduling, fairness, latency, and crash
evidence receives explicit human review; defining the semantic mode and measuring
it does not enable it. Its possible MVP default remains a post-POC decision.

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
   same session. An absent pointer is a valid initialization state only while the
   contract-bundle table and every application-authoritative table are empty;
   capability, bootstrap, and administration-audit records may already exist.
   Any bundle, application commit/state, active-pointer mismatch, unknown plan,
   or semantic validation failure outside that state is corruption.
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
9. Rebuild authoritative in-memory indexes, lock metrics, and subscriptions only
   after the matching `StructurallyOpened`/`ValidatedCatalogHistory` pair is accepted.

`StructurallyOpened` alone never means readiness. WP-130 authoritative readiness
becomes true only after matching same-session `ValidatedCatalogHistory`, digest-provider
inventories, and all remaining checks succeed, both allocators can progress, and
an active contract is valid. A truly empty database may pass structural and
catalog integrity while remaining in `Initializing`, not ready. In that mode the
server exposes health plus the exact loopback bootstrap operation; after
bootstrap it additionally permits authenticated contract deployment through the
ordinary service/coordinator path. No command or general read path is enabled
until deployment atomically establishes the active contract.

Derived-component recovery is separate and cannot rewrite authoritative source
state:

1. WP-070 reports bounded structural findings for projection rows/control/
   markers and outbox delivery status without applying worker policy or blocking
   otherwise healthy authoritative writes solely for a derived fault.
2. WP-160 interprets an outbox intent with no status row as never-attempted
   `Pending` and idempotently normalizes a prior `Delivering` status to the
   policy's retryable pending state before the dispatcher becomes ready.
3. WP-170 verifies each retained projection generation's reciprocal contiguous
   markers, apply hashes, lifecycle, pointers, and state keys. A component fault
   is `Degraded`; recovery retires/quarantines the failed candidate as specified
   and rebuilds only into a fresh generation. It never repairs commits/entities
   from projection state or lowers a published frontier.
4. WP-185 reports projection/outbox recovery failures as component degradation
   while preserving the distinct authoritative readiness signal. A shared engine
   corruption that prevents trustworthy table isolation remains an authoritative
   storage failure, not a derived exception.

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

Adapters own transport decode, credential extraction, checked conversion,
deadline/cancellation propagation, and total result mapping. The service owns
semantic validation, current authorization, approved provenance, audit
orchestration, obligations/redaction, and safe release. Typed WP-100 executors
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

## 11.2 Services

```protobuf
service ContractService {
  rpc ValidateContract(ValidateContractRequest) returns (ValidateContractResponse);
  rpc ExplainCommand(ExplainCommandRequest) returns (ExplainCommandResponse);
  rpc DeployContract(DeployContractRequest) returns (DeployContractResponse);
  rpc GetActiveContract(GetActiveContractRequest) returns (GetActiveContractResponse);
}

service CommandService {
  rpc Execute(ExecuteCommandRequest) returns (ExecuteCommandResponse);
  rpc GetOutcome(GetOutcomeRequest) returns (GetOutcomeResponse);
}

service QueryService {
  rpc GetEntity(GetEntityRequest) returns (GetEntityResponse);
  rpc ScanIndex(ScanIndexRequest) returns (ScanIndexResponse);
  rpc QueryProjection(QueryProjectionRequest) returns (QueryProjectionResponse);
}

service CommitService {
  rpc GetCommit(GetCommitRequest) returns (GetCommitResponse);
  rpc ScanCommits(ScanCommitsRequest) returns (ScanCommitsResponse);
  rpc SubscribeCommits(SubscribeCommitsRequest) returns (stream CommitNotification);
}

service AdminService {
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc Stats(StatsRequest) returns (StatsResponse);
  rpc CreateCapability(CreateCapabilityRequest) returns (CreateCapabilityResponse);
  rpc RevokeCapability(RevokeCapabilityRequest) returns (RevokeCapabilityResponse);
}
```

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

For `EXECUTED_READ_ONLY`, `commit_sequence` is the wire sentinel zero and
`provenance_uri` and `durability_mode` are empty. Adapters map these to semantic
absence and never construct `CommitSequence(0)`, a provenance identity, or a
durability mode. `COMMITTED` and `REPLAYED` require their original nonzero
sequence and complete terminal fields. Unknown status values and every
status/field inconsistency reject. This status gives no durable replay or
outcome-recovery promise.

The custom `Value` family is required; `google.protobuf.Struct` is forbidden at exact business-value boundaries because it cannot preserve all signed/unsigned integer and decimal values. Lists, records, strings, and bytes inherit compiled or protocol bounds. Record fields MUST be unique and canonically ordered by stable field ID where available, then by UTF-8 name. Decimal coefficient, scale, currency, UUID, date, and timestamp encodings MUST be validated and canonical before hashing or persistence.

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

Before WP-130 exposes the ADR-0012 error, the public error registry additively
includes `PUBLIC_ERROR_KIND_COMMAND_EXECUTION_FAILED = 9`, stable code
`command_execution_failed`, static message `command execution failed`, recovery
`CONTACT_OPERATOR`, and required `CommandExecutionFailureDetails` in
`PublicError.execution_failure = 8`. Its required closed code is arithmetic
fault `1` or resource limit `2`; zero and unknown values reject. gRPC maps it to
`FAILED_PRECONDITION`. No existing error value or field is renumbered.

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
- Compression is disabled for small command messages and MAY be enabled for contract bundles or scans.

## 11.5 Rust SDK

The POC MUST provide:

- A generic client capable of invoking commands by name and dynamic typed value.
- Generated Rust input structs and outcome enums for the example contract.
- Automatic idempotent retry on safe transport failures using the same canonical
  command input and idempotency key with a fresh `RequestId` for each submission.
- An explicit `OutcomeUnknown` client error when automatic resolution cannot complete.
- `wait_for_projection(sequence)` helpers.
- Trace-context propagation.

Generated SDK code MUST not contain database correctness logic. It validates ergonomics and shapes data; the server remains authoritative.

---
# 12. Native Model Context Protocol interface

## 12.1 Design position

MCP is a first-class product interface because the database is intended for agent-driven development and operation. The MCP layer is generated from the same active contract bundle and uses the same service, authorization, policy, command runtime, and provenance paths as gRPC.

The POC baseline is MCP protocol version **2025-11-25** and the official Rust SDK `rmcp` 2.2.x, verified on 2026-07-12. MCP protocol and SDK use MUST be isolated behind `riffdb-api-mcp` so future protocol revisions do not leak into compiler, runtime, or storage crates.

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

### Streamable HTTP

`riffdbd` exposes a native `/mcp` Streamable HTTP endpoint.

- POC: disabled by default, loopback binding only, opaque development capability token.
- MVP: remote use over TLS, OAuth 2.1 protected-resource behavior, audience-bound access tokens, tenant policy, rate limits, and audit.

`MCP-010` Stdio and Streamable HTTP MUST expose equivalent authorized tools and resources for the same principal and active contract.

`MCP-011` Transport-specific authentication MUST call the same auth-owned
`CredentialAuthenticator`, produce the same privately constructed
`AuthenticatedPrincipal`, and enter the same service-owned authorization and
obligation path. MCP adapters MUST NOT construct an actor or capability decision.

## 12.3 Advertised capabilities

### POC server capabilities

- Tools with `listChanged=true`.
- Resources with `listChanged=true` and selected subscriptions.
- Prompt templates for operational workflows MAY be included after tools and resources are stable.
- Progress for contract validation, deployment analysis, branch replay, and projection rebuild operations.
- Cancellation support with the command-boundary semantics in Section 9.7.

### Explicitly deferred MCP capabilities

- Client sampling initiated by the database.
- Roots.
- Elicitation.
- Experimental or extension-based task execution.
- MCP Apps.

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

These naming rules accept only ADR-0020. Resource URI encoding, HTTP audience,
stdio-over-gRPC transport, cursor presentation, and their exact compatibility
fixtures remain Proposed under ADR-0008 and are not frozen by this section.

### Generated tool definition

```json
{
  "name": "riffdb.cmd.legalspend.allocatebudget",
  "title": "Allocate Budget",
  "description": "Atomically allocate an amount from an organization's annual budget. Requires a caller-supplied idempotency key. Returns a declared business outcome and commit sequence.",
  "inputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": false,
    "properties": {
      "idempotency_key": { "type": "string", "minLength": 1, "maxLength": 128 },
      "organization_id": { "type": "string", "format": "uuid" },
      "fiscal_year": { "type": "integer" },
      "matter_id": { "type": "string", "format": "uuid" },
      "amount": { "type": "string", "pattern": "^-?[0-9]+\\.[0-9]{2}$" }
    },
    "required": [
      "idempotency_key",
      "organization_id",
      "fiscal_year",
      "matter_id",
      "amount"
    ]
  },
  "outputSchema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "additionalProperties": false,
    "properties": {
      "status": { "enum": ["committed", "replayed"] },
      "commit_sequence": { "type": "string", "pattern": "^[0-9]+$" },
      "contract_version": { "type": "integer" },
      "outcome": {
        "oneOf": [
          {
            "type": "object",
            "properties": {
              "type": { "const": "Allocated" },
              "remaining": { "type": "string" }
            },
            "required": ["type", "remaining"],
            "additionalProperties": true
          },
          {
            "type": "object",
            "properties": {
              "type": { "const": "InsufficientBudget" },
              "approved": { "type": "string" },
              "allocated": { "type": "string" },
              "requested": { "type": "string" }
            },
            "required": ["type", "approved", "allocated", "requested"],
            "additionalProperties": false
          },
          {
            "type": "object",
            "properties": {
              "type": { "const": "InvalidAmount" },
              "minimum": { "type": "string" }
            },
            "required": ["type", "minimum"],
            "additionalProperties": false
          }
        ]
      },
      "provenance_uri": { "type": "string", "format": "uri" }
    },
    "required": [
      "status",
      "commit_sequence",
      "contract_version",
      "outcome",
      "provenance_uri"
    ]
  }
}
```

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
| `riffdb.outbox.list_pending` | Administrative read | Optional POC | Inspect pending deliveries without payload fields the caller cannot read. |
| `riffdb.server.health` | Read-only | Required | Return readiness, active contract, last commit, and degraded components. |

Administrative tools MUST be absent from `tools/list` when the session lacks permission, rather than merely failing after selection.

## 12.6 Resource model

MCP resources provide application-controlled context. Resources are redacted by the current principal and can be returned as links from tools.

### Resource URIs

| URI template | MIME type | Contents |
|---|---|---|
| `riffdb://contract/active` | `application/json` | Active contract metadata and links |
| `riffdb://contract/{version}` | `application/json` | Bundle summary, source hash, plan hash, compatibility metadata |
| `riffdb://entity/{entity}/schema` | `application/schema+json` | Entity JSON Schema and field policy metadata |
| `riffdb://command/{command}/plan` | `application/json` | Explain plan and generated schemas |
| `riffdb://command/{command}/docs` | `text/markdown` | Generated command documentation and examples |
| `riffdb://outcome/{principal}/{command}/{key_hash}` | `application/json` | Authorized persisted outcome for uncertainty recovery |
| `riffdb://commit/{sequence}` | `application/json` | Redacted commit record |
| `riffdb://provenance/{provenance_id}` | `application/json` | Actor, agent session, source, approval, contract, and affected aggregates |
| `riffdb://projection/{projection}/status` | `application/json` | Lifecycle, frontier, lag, and error state |
| `riffdb://server/health` | `application/json` | Health and readiness information |

`MCP-030` Resource list and content MUST be filtered by capability and field policy.

`MCP-031` Resource subscriptions SHOULD be implemented for active contract, command plan, projection status, and server health resources.

`MCP-032` Contract deployment MUST trigger resource list or update notifications when negotiated.

`MCP-033` Resource reads MUST enforce bounded payload sizes and MAY return resource links to paginated detail instead of embedding large data.

## 12.7 Prompt templates

Prompts are user-controlled MCP primitives and are not required for the first command vertical slice. The POC SHOULD add these once tool behavior is stable:

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

## 12.9 Progress and cancellation

Long-running MCP operations SHOULD send progress when the client provided a progress token:

- Contract compilation across multiple files.
- Compatibility and replay analysis.
- Contract deployment with projection initialization.
- Projection rebuild.
- Branch replay in MVP.

Progress values MUST be monotonic. Progress notifications stop after completion or cancellation.

Cancellation behavior follows Section 9.7. For a mutating command, the MCP adapter MUST document that cancellation after commit submission may not prevent the commit. The idempotency outcome resource is the recovery mechanism.

## 12.10 Pagination

MCP list and scan operations use opaque cursor pagination.

- Cursor content is signed or server-stored; clients MUST NOT be able to alter partition, policy, or filter state.
- Default page size: 50.
- Maximum page size: 500 for administrative commit scans and lower for entity data when policy requires.
- Cursors include active contract version and expire after configuration-defined time.

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
- Pin `rmcp` minor versions in the workspace lockfile.
- Maintain an internal adapter trait so a future MCP protocol revision can coexist during migration.
- The POC MUST report its supported protocol version and server implementation metadata accurately during initialization.

---

# 13. Authorization, capabilities, and provenance

## 13.1 Actor context

Transport authentication returns a privately constructible
`AuthenticatedPrincipal` containing the stable principal, actor kind, current
capability identity/revision, audience, tenant scope, and authentication time,
but no raw token or digest material. The adapter combines it with one request ID,
trusted ingress, bounded untrusted provenance claims, deadline/cancellation, and
trusted trace propagation into the service-owned checked `RequestContext`.

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
| `riffdb-mcp` | Stdio MCP bridge that authenticates locally and invokes the shared `riffdbd` service. It contains no storage or command semantics. |

The POC ships exactly these three binaries. Deterministic code generation, when needed, is a `riffdb contract generate` subcommand. Replay, durable inspection, and recovery helpers remain test-harness code in the POC; a privileged offline repair binary is deferred to Stage A and requires a separate authorization and audit design.

## 16.2 Configuration precedence

Configuration precedence, highest first:

1. Explicit CLI flags.
2. Environment variables prefixed `RIFFDB_`.
3. A TOML configuration file.
4. Built-in safe defaults.

Secrets MUST NOT be accepted from the TOML file unless the file mode and deployment environment satisfy a documented local-development policy. Production-grade secret delivery is an MVP concern.

Representative configuration:

```toml
[server]
data_dir = "./data"
grpc_listen = "127.0.0.1:7443"
mcp_http_enabled = false
mcp_http_listen = "127.0.0.1:7444"
max_request_bytes = 1048576
shutdown_grace_ms = 10000

[storage]
engine = "redb"
durability = "sync"
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
metrics_listen = "127.0.0.1:9464"
```

Configuration parsing MUST reject unknown keys by default. Every setting MUST document whether changing it requires restart and whether it affects correctness, compatibility, security, or only performance.

## 16.3 Health model

`riffdbd` exposes:

- **Liveness**: process event loop is functioning.
- **Readiness**: storage opened, active contract loaded, commit coordinator accepting work, and required migration checks complete.
- **Degraded state**: core writes remain available but a non-authoritative subsystem such as projection or outbox is unhealthy.

`Initializing` is distinct from corruption and readiness. A structurally empty
database without an active contract reports not ready while exposing only health,
one-time loopback bootstrap while the marker is absent, and authenticated first
contract deployment after bootstrap. The deployment still uses the shared
service, authorization, audit, and coordinator. Once an active contract exists,
its later absence or mismatch is a fail-closed authoritative integrity error.

WP-130 owns this minimum production lifecycle and the first runnable `riffdbd`:
identity initialization through the commit executor, complete redb structural
evidence, matching catalog historical validation, readable capability and
idempotency digest inventories, bootstrap, deployment, active readiness, gRPC,
and clean restart. WP-185 does not replace that gate. It extends the same
component graph with MCP HTTP, outbox/projection workers, observability, and
separate derived-health aggregation; a derived failure cannot retroactively
invalidate the accepted authoritative startup proof.

```rust
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

## 16.6 Administrative CLI examples

```bash
riffdb contract validate contracts/budget.riff
riffdb contract deploy contracts/budget.riff --expected-version 0
riffdb command execute AllocateBudget --input request.json
riffdb command outcome AllocateBudget --idempotency-key 81e6...
riffdb entity get Budget --key organization_id=... --key fiscal_year=2026
riffdb commit show 42
riffdb projection query BudgetUtilizationDaily --after 42 --input query.json
riffdb capability create --profile local-agent --expires-in 8h
riffdb server health
```

CLI JSON output MUST be stable enough for integration scripts. Human-formatted output is additive and MUST not replace a machine-readable mode.

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
| During execution-failure terminalization | Either the equal-evidence `ExecutionFailed` admission is durable with no application sequence/provenance/event/commit, or the original pending admission remains; unknown status fences writes and same-key recovery resolves it. |
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
- Snapshot and backup manifest parsers.

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

Online backup, point-in-time recovery, incremental backup, encrypted backup, and remote object storage are MVP work.

## 18.6 Release artifacts

POC release artifacts:

- Exactly `riffdbd`, `riffdb`, and `riffdb-mcp`.
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
13. WP-020, the exact ADR-0012 WP-010 public-error carve-out, WP-065, and WP-127
    are the only schema-owner phases. The carve-out may add no request, result,
    service, RPC, or durable-record field. WP-060/WP-070 cannot guess durable
    fields, and WP-120/WP-130 cannot guess public fields or move service
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
| `WP-050` | Contract catalog | WP-020, WP-040, WP-060 | Bundle lookup, catalog state semantics, compatibility report, same-session historical IR validation proof, and typed expected-version deployment operation for later coordinator routing | Catalog semantic, exact-end historical-validation, and atomic-storage-operation tests pass; final routing evidence is WP-100/WP-120 |
| `WP-060` | Storage semantic API | WP-010, WP-020, WP-040 | Owned snapshots, dependencies, `EvaluatedCommand`/`CommitIntent`, the pre-sequence write-plan/capacity type-state, exact `EventHash` semantics, structural evidence/type-state ports, semantic durable DTOs, typed persistence transitions including audit/capability/projection, and in-memory reference implementation | Reference-model, exact-end startup type-state, pre-sequence ordering/capacity, event-hash, and semantic conformance tests pass |
| `WP-065` | Durable semantic record schema | WP-020, WP-060 | Exact 26-record `riffdb.storage.v1` registry including standalone durable events, canonical envelopes and per-class upper-bound proofs, exact event-hash goldens, descriptors, schema hashes, wire validation, historical registrations, and checked storage-owned Proto mappings | Proto/storage size/hash tests and clean deterministic regeneration pass |
| `WP-070` | Redb storage engine | WP-020, WP-060, WP-065 | Frozen canonical tables/keys including standalone events, ordered coordinator write transaction with pre-sequence reservation and pre-stage canonical-envelope checks, atomic records, indexes, capability/audit/projection persistence, complete structural evidence scan, `StructurallyOpened` dormant ports, recovery, SHA-256 backup, and dependency-free benchmarks | Storage properties and core process recovery matrix pass without claiming IR-aware readiness |
| `WP-075` | Fjall semantic comparison | WP-060, WP-070 | Isolated non-production Fjall adapter and unchanged conformance/benchmark harness | Conformance report and reproducible evidence pass |
| `WP-080` | Deterministic command runtime | WP-040, WP-060 | Shared pure expression evaluator plus predicate, invariant, postcondition, and commit-check evaluation; snapshot IR interpreter, fixed logical time and budget, dependencies, `EvaluatedCommand`, and closed post-snapshot execution faults; no contract state-machine IR/execution, provenance, or command randomness | Differential tests against model pass |
| `WP-090` | Conflict manager | WP-010 | Canonical multi-key exclusive acquisition, cancellation, metrics | Loom/Shuttle suites pass |
| `WP-100` | Commit coordinator and idempotency | WP-050, WP-070, WP-080, WP-090, WP-110 | Admission reservation, revalidation, mutation-affected epoch planning, pre-sequence capacity reservation, verified sequence-derived event/record graph, atomic outcome/event/provenance/outbox-intent commit, and typed control-plane operations | Concurrency, pre-sequence failpoints, core process crash, and lost-response replay tests pass |
| `WP-110` | Authorization and capability service | WP-010, WP-070 | Opaque capability records, policy decisions, obligations, and typed create/revoke operations for later coordinator routing | Policy, digest, revocation-state, and fail-closed tests pass; final non-bypass evidence is WP-120 |
| `WP-120` | API-neutral application service | WP-050, WP-080, WP-100, WP-110 | Six operation-specific service traits, checked contexts/DTOs, pure input/partition/conflict preparation with fail-closed pre-admission arithmetic, current policy, audit orchestration, obligations, bounded reads/waits/streams, and server-side cursors | In-process end-to-end and no-storage-bypass tests pass |
| `WP-125` | Service comparison adapter | WP-045, WP-120 | Budget workload adapter over the API-neutral service | Shared oracle passes against in-process RiffDB |
| `WP-127` | Public Protobuf API completion | WP-020, WP-120 | Complete all remaining supported `riffdb.v1` messages, preserve the WP-010 execution-failure slice, and freeze descriptors, schema hashes, wire validation, and golden/client fixtures; no service conversion code | Proto tests and clean deterministic regeneration pass |
| `WP-130` | gRPC server and Rust SDK | WP-020, WP-120, WP-127 | Tonic services/client plus the minimal production redb/catalog/commit/auth/policy/service composition, concrete ID/clock/cursor/digest providers, staged startup/lifecycle, and real restart proof | Public API conformance and the child-process temporary-redb bootstrap/deploy/budget/restart suite pass |
| `WP-135` | Public SDK comparison adapter | WP-125, WP-130 | Budget comparison through the public Rust SDK/gRPC path | Shared oracle passes through the canonical client path |
| `WP-140` | Native MCP interface | WP-040, WP-120, WP-130 | `rmcp` stdio-over-gRPC and HTTP adapters, dynamic tools/resources, schema results, progress/cancellation | MCP conformance and generated-tool tests pass |
| `WP-150` | CLI and local developer flow | WP-130 | `riffdb`, config, local token flow, JSON/human output | Acceptance steps executable without internal APIs |
| `WP-160` | Durable outbox | WP-100, WP-120 | Event scanner, delivery state, test connector, retry and duplicate simulations | Crash and duplicate-delivery tests pass |
| `WP-170` | Projection core | WP-100, WP-120 | Event consumers, count/sum state, durable frontier, read-after-sequence query | Prefix and restart properties pass |
| `WP-180` | Observability and diagnostics | WP-120 | Tracing, metrics, health, explain output, redaction | Required telemetry present without sensitive labels |
| `WP-185` | Server composition | WP-130, WP-140, WP-160, WP-170, WP-180 | Extend the runnable WP-130 `riffdbd` graph with MCP HTTP, outbox, projection, observability, and derived health without replacing P1 startup/lifecycle providers | Extended composition tests pass while retaining the WP-130 restart proof |
| `WP-190` | Integrated crash harness | WP-070, WP-100, WP-160, WP-170, WP-185 | Named failpoints, process controller, recovery assertions | Full cross-component failpoint matrix passes |
| `WP-200` | POC acceptance and release | All POC packages | One-command demo, benchmark report, security notes, release artifacts | POC-001 through POC-010 signed off |

WP-045, WP-075, WP-125, and WP-135 are evidence tracks rather than dependencies of the production semantic kernel. They may not add PostgreSQL or Fjall dependencies to the main workspace. WP-200 consumes their reports and runner, but a failure in a comparison implementation does not weaken or redefine RiffDB semantics.

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
| K | WP-125, WP-127, WP-160, WP-170, WP-180 |
| L | WP-130 |
| M | WP-135, WP-140, WP-150 |
| N | WP-185 |
| O | WP-190 |
| P | WP-200 |

Within a wave, agents MUST coordinate changes to shared types through interface PRs before parallel implementation PRs.

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

The long-lived budget comparison workload, deterministic oracle, and PostgreSQL baseline begin after the compiler as a non-production evidence track. They are not P0 gate members, but their public-adapter and benchmark evidence must be complete before WP-200 closes P2.

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
  ID/clock/cursor/digest providers and staged same-session startup validation.
- gRPC, Rust SDK, CLI.
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
WP-110, WP-120, WP-127, WP-130, and WP-150. WP-065 and WP-127 are explicit
gate members as well as hard dependencies of WP-070 and WP-130; they do not
replace any previously listed member.

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
- Full POC demo and handoff documentation.

**Gate P2 / POC exit:** `POC-001` through `POC-010` pass; known limitations are explicit; architecture review confirms the semantics are worth productizing.

## 20.5 Stage A — single-node alpha hardening

**Objective:** Turn the prototype into a stable, supportable single-node alpha for trusted design partners.

**Additions:**

- Stable storage-format migration framework.
- Online consistent backup and verified restore.
- Contract language versioning and bundle signing.
- More entity indexes and bounded indexed reads.
- Operational repair commands with two-person or approval policy and complete audit.
- Remote MCP over TLS with standards-based authorization.
- Generated TypeScript and Python clients.
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

Risk owners are assigned in the project tracker. A risk may be closed only with evidence or an accepted product decision, not by removing it from the register.

---

# 22. Architecture decision records and open decisions

## 22.1 Required ADRs

Specification v0.13 records each ADR's current status. An Accepted record is
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
| `ADR-0008` | Proposed | Native MCP resource URIs, audiences, stdio-over-gRPC model, cursor presentation, and remaining protocol fixtures | WP-140 fixtures |
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

## 22.2 Decisions to resolve before implementation reaches the named gate

ADR-0015 resolved initial entity creation: the canonical `CreateBudget` compiled
command seeds the demo through the ordinary coordinator path, and direct
storage/admin seeding remains forbidden. The accepted ADR-0004/ADR-0012 batch
also resolved the remaining grammar-v1 transaction defaults:

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
  `ExecutionFailed` only after complete dependency equality; they receive no
  application sequence or command provenance and use the exact public error and
  uncertainty rules in that ADR.
- POC contract compatibility is additive/documentation-only; removal of an
  entity, field, command, outcome, event, or projection is rejected.
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
  "outcome": {
    "type": "Allocated",
    "remaining": "20.00"
  },
  "provenance_uri": "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23"
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

The demo script exits non-zero when any assertion fails and writes a JSON report containing requirement IDs, command sequences, outcome types, and integrity status.

---

# 24. Definition of done

## 24.1 POC definition of done

The POC is done only when:

- All `POC-*` criteria pass in CI from a clean checkout.
- The budget acceptance demo passes through gRPC, Rust SDK, CLI, and MCP.
- The crash matrix covers every authoritative transaction boundary.
- The reference-model and generated-history suites run without invariant divergence.
- Storage, protocol, contract IR, and MCP compatibility policies are documented.
- There is no undocumented mutation or administrative bypass.
- A threat model and dependency audit are published.
- Benchmark methodology and results are reproducible.
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
index_epochs/<exact-canonical-index-range-prefix>
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
}

service CommandService {
  rpc Execute(ExecuteCommandRequest) returns (ExecuteCommandResponse);
  rpc GetOutcome(GetOutcomeRequest) returns (GetOutcomeResponse);
}

service QueryService {
  rpc GetEntity(GetEntityRequest) returns (GetEntityResponse);
  rpc ScanIndex(ScanIndexRequest) returns (ScanIndexResponse);
  rpc QueryProjection(QueryProjectionRequest) returns (QueryProjectionResponse);
}

service CommitService {
  rpc GetCommit(GetCommitRequest) returns (GetCommitResponse);
  rpc ScanCommits(ScanCommitsRequest) returns (ScanCommitsResponse);
  rpc SubscribeCommits(SubscribeCommitsRequest) returns (stream CommitNotification);
}

service AdminService {
  rpc Health(HealthRequest) returns (HealthResponse);
  rpc Stats(StatsRequest) returns (StatsResponse);
  rpc CreateCapability(CreateCapabilityRequest) returns (CreateCapabilityResponse);
  rpc RevokeCapability(RevokeCapabilityRequest) returns (RevokeCapabilityResponse);
}
```

This appendix intentionally repeats the five canonical services from Section 11.2. A sixth entity or projection service, or abbreviated RPC aliases, is not part of v0.6. Public messages MUST use stable field numbers, reserve removed fields, bound nested sizes, use the exact `Value` family in Section 11.3, and distinguish absent values from defaults when semantics require it.

---

# Appendix C. Error taxonomy

| Layer | Example | Public representation |
|---|---|---|
| Declared business outcome | `InsufficientBudget` | Normal typed outcome; not transport error |
| Input/schema error | Invalid UUID or unknown field | Validation error with bounded field diagnostics |
| Idempotency misuse | Same key, different canonical input | Stable conflict error containing no previous sensitive input |
| Authorization | Command or field not allowed | Permission denied; tool may also be hidden from catalog |
| Concurrency admission | Lock deadline exceeded | Retryable execution error with safe retry guidance |
| Contract mismatch | Client invokes command absent from active bundle | Failed precondition with active version and refresh hint |
| Storage unavailable | Commit cannot be made durable | Unavailable/internal class; no success claim |
| Outcome uncertainty | Client lost response after submission | Client resolves through same idempotency key or `GetOutcome` |
| Deterministic command execution failure | Dependency-validated arithmetic or fixed resource-limit fault | `CommandExecutionFailed` with closed code and `CONTACT_OPERATOR`; no application commit or sequence |
| Projection lag | Required sequence not reached by deadline | Typed projection `WaitTimedOut` result |
| Projection degraded | Worker cannot advance | Typed degraded result with safe reason |
| Internal bug | Runtime invariant or impossible state | Opaque incident ID, server-side diagnostics, fail closed |

Errors MUST make it possible to distinguish safe retry, same-key outcome resolution, caller correction, permission escalation request, and operator intervention.

---

# Appendix D. Source and dependency references

The implementation MUST prefer primary project documentation and pin reviewed versions in the workspace rather than treating the versions below as unbounded ranges.

1. Agentic OLTP Database — Concept Design, Draft, July 2026.
2. Model Context Protocol specification, version 2025-11-25: architecture, lifecycle, tools, resources, transports, authorization, cancellation, and progress.
3. Official Model Context Protocol Rust SDK (`rmcp`).
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

Every normative requirement MUST be traceable to at least one automated test, review checklist item, or explicitly justified manual verification artifact before its stage can pass.
