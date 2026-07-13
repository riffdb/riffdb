# RiffDB
## Standalone Rust POC Technical Specification

### Native MCP interface and roadmap to MVP

**Tagline:** *Vibe fast. Commit safely.*  
**Category:** Contract-first operational database for agent-built applications  

**Version:** 0.3
**Status:** Architecture-approved implementation handoff draft  
**Date:** 13 July 2026
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
| Single commit sequence | `LOG-001` Every terminal admitted command result MUST receive a monotonically increasing `CommitSequence`, including persisted no-op business outcomes. | Unifies idempotency, provenance, change consumption, and projection frontiers. |
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
| Command application service | API-neutral orchestration, idempotency lookup, plan selection, lock acquisition, runtime invocation, commit submission | Transport-specific response formatting |
| Deterministic runtime | Evaluate a compiled command plan against a snapshot and produce a `CommitIntent` | I/O, clocks, random OS state, network calls |
| Conflict manager | Canonical conflict-key acquisition, wait queues, cancellation, fairness, hot-key metrics | Durable state |
| Commit coordinator | Serialize durable commands and control-plane operations, assign application or administration sequences, revalidate, and apply records atomically | Contract parsing or external effects |
| Storage engine | Atomic key/value transactions and ordered scans required by the semantic layer | Business rules |
| Commit log and provenance | Durable ordered record of terminal command attempts and mutations | Projection-specific state |
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
  examples/
    budget-comparison/       # long-lived workload specification and documentation
    postgres-budget/         # nested non-production PostgreSQL comparison workspace
    fjall-storage/           # nested non-production Fjall comparison workspace
  scripts/
  docs/
```

The binary targets are `riffdbd` from `riffdb-server`, `riffdb` from `riffdb-cli`, and `riffdb-mcp` from `riffdb-mcp-stdio`.

## 5.2 Crate responsibilities

| Crate | Public responsibility | Allowed dependency direction |
|---|---|---|
| `riffdb-types` | Stable identifiers, canonical value types, versions, timestamps, decimals, and common value semantics | Foundation crate; no storage or transport dependencies |
| `riffdb-errors` | Public-safe errors, internal error layering, redaction boundaries, and incident identifiers | Types only; no transport-specific errors in core layers |
| `riffdb-proto` | Generated Protobuf types, durable envelopes, and conversion helpers | Types plus Prost; no runtime semantics |
| `riffdb-contract-syntax` | Lexer, parser, source spans, syntax AST, and parser diagnostics | Types and parser tooling only |
| `riffdb-contract-ir` | Typed HIR, executable IR, plans, schemas, and contract bundle structs | Types; no parser implementation or runtime dependency |
| `riffdb-contract-compiler` | Name resolution, type checking, invariant classification, locality analysis, plan generation, and JSON Schema output | Syntax and IR; no storage, service, or transport dependency |
| `riffdb-catalog` | Immutable contract bundle persistence, compatibility checks, active-version changes, and catalog notifications | IR and storage API |
| `riffdb-storage-api` | Narrow semantic interfaces for snapshots, atomic command commits, scans, catalog state, and derived workers | Types and durable record definitions; no concrete engine |
| `riffdb-storage-memory` | Deterministic reference storage implementation used by model and semantic tests | Storage API only |
| `riffdb-storage-redb` | Durable POC implementation, table layout, integrity checks, backup, restore, and engine benchmarks | Storage API and `redb`; no API transports |
| `riffdb-invariant` | Evaluation of supported predicates, transitions, postconditions, and commit-time invariant checks | Types and IR |
| `riffdb-runtime` | Deterministic command-plan interpreter that produces typed outcomes and `CommitIntent` values without external I/O | IR, invariant engine, and read-snapshot traits |
| `riffdb-conflict` | Canonical conflict keys, exclusive logical capabilities, wait queues, cancellation, and hot-key diagnostics | Types and synchronization primitives; no storage engine |
| `riffdb-idempotency` | Canonical command identity, input hashing, persisted outcome lookup, duplicate detection, and uncertain-result recovery | Types and storage API |
| `riffdb-commit` | Admission orchestration, capability acquisition, dependency revalidation, commit-sequence assignment, atomic persistence, and ordered commit records | Runtime, conflict, idempotency, catalog, and storage API |
| `riffdb-auth` | Principal authentication, local development capability tokens, expiry, and credential resolution | Types and errors; no command execution |
| `riffdb-policy` | Deny-by-default authorization, capability scopes, tenant and field filtering, redaction obligations, and approval decisions | Auth and types; transport-neutral |
| `riffdb-service` | API-neutral command, contract, entity, commit, provenance, projection, and health services | Core semantic crates only; no transport implementation |
| `riffdb-api-grpc` | Tonic services, interceptors, bounds checks, and wire conversions | Service and proto; no storage implementation |
| `riffdb-client-rust` | Generic and generated Rust client APIs | Proto and Tonic client only |
| `riffdb-server` | `riffdbd` process composition, configuration, lifecycle, and hosted gRPC/HTTP endpoints | Service and API crates; no new business semantics |
| `riffdb-api-mcp` | MCP tool/resource catalogs, schema translation, authorization-aware discovery, Streamable HTTP handling, and protocol adaptation | Service, policy, `rmcp`, and JSON Schema support |
| `riffdb-mcp-stdio` | `riffdb-mcp` local stdio bridge that invokes the shared public service | MCP client/server transport glue only; no storage access |
| `riffdb-cli` | `riffdb` operator and developer CLI using public APIs | Rust client and bounded local configuration only |
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
| Embedded storage | redb 4.1.x | POC durable state and atomic commits |
| Storage comparison | Fjall 3.1.x | POC-exit benchmark and possible MVP engine |
| MCP SDK | rmcp 2.2.x | Native MCP server, stdio, Streamable HTTP |
| Lexer | Logos 0.16.x | Contract tokenization |
| Parser | LALRPOP 0.23.x | Contract grammar |
| Diagnostics | Miette | Compiler errors with source spans and stable diagnostic codes |
| Tracing | tracing 0.1.x | Structured spans and events |
| Property testing | Proptest 1.x | Generated contracts, values, and histories |
| Exhaustive concurrency | Loom 0.7.x | Small lock-manager and notification components |
| Randomized concurrency | Shuttle 0.9.x | Larger command and task schedules |
| Benchmarks | Criterion | Stable local micro and scenario benchmarks |
| Future replication | OpenRaft 0.9.x behind a facade | MVP replicated state machine; not linked into POC server |

---
# 6. Core domain model and identifiers

## 6.1 Stable identifiers

The semantic model MUST use newtypes rather than raw strings or integers at component boundaries.

```rust
pub struct ContractVersion(pub u64);
pub struct PlanHash(pub [u8; 32]);
pub struct ProjectionPlanHash(pub [u8; 32]);
pub struct ContractPlanRootHash(pub [u8; 32]);
pub struct CommitSequence(pub u64);
pub struct RequestId(pub uuid::Uuid);
pub struct ActorId(pub String);
pub struct AgentSessionId(pub uuid::Uuid);
pub struct EntityTypeId(pub u32);
pub struct EventTypeId(pub u32);
pub struct EnumTypeId(pub u32);
pub struct EnumVariantId(pub u32);
pub struct AggregateTypeId(pub u32);
pub struct FieldId(pub u32);
pub struct CommandId(pub u32);
pub struct OutcomeId(pub u32);
pub struct ProjectionId(pub u32);
pub struct IndexId(pub u32);
pub struct InvariantId(pub u32);
pub struct EntityKey(pub Vec<u8>);
pub struct PartitionKey(pub Vec<u8>);
pub struct ConflictKey(pub Vec<u8>);
pub struct IndexEntryKey(pub Vec<u8>);
pub struct PartitionKeyHash(pub [u8; 32]);
pub struct ConflictKeyHash(pub [u8; 32]);
pub struct CanonicalInputHash(pub [u8; 32]);
pub struct IdempotencyKey(pub String);
```

`RequestId` identifies one transport submission and is used for tracing. `IdempotencyKey` is a caller-selected command input used to recover an uncertain result. They are distinct values: retrying an uncertain command MAY use a new `RequestId` but MUST reuse the original `IdempotencyKey` and canonical command input.

### Identifier requirements

- `ID-001` Externally created request and agent-session identifiers MUST be UUIDv7 or another time-sortable 128-bit identifier approved by ADR.
- `ID-002` `CommitSequence` MUST be a contiguous unsigned 64-bit integer on a single node.
- `ID-003` Compiler-assigned numeric IDs MUST be stable within a contract lineage and MUST NOT be reused after removal.
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
    pub entity_version: u64,
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

The language is not a general-purpose programming language. It is a small, terminating, typed description of persistent entities, aggregate ownership, commands, invariants, state transitions, durable events, queries, and simple projections.

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

### State machines

State machine syntax is optional for the first vertical slice but MUST be represented in the IR model. Direct writes to a state-machine field are illegal; only declared transition instructions may change it.

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

Before evaluation, the commit coordinator durably creates or resolves a pending idempotency reservation containing the identity, canonical input hash, request ID, contract version, plan hash, and fixed `tx.time`. A pending reservation receives no `CommitSequence`. A retry with the same identity and input resumes or waits for that reservation and reuses its `tx.time` and plan; a retry with a different input fails without execution. A terminal declared rejection receives one sequence like any other terminal admitted outcome. Returning a persisted outcome on replay allocates no new sequence.

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
    pub compatibility: CompatibilityReport,
    pub compiler_version: String,
    pub ir_format_version: u32,
}
```

`CMP-020` Bundle serialization MUST be deterministic. Compiling identical source with the same compiler version MUST produce byte-identical bundles and the same plan hash. Wall-clock compilation or deployment time MUST NOT appear in a bundle.

`CMP-021` The bundle MUST record compiler version and IR format version separately from application contract version.

`CMP-022` The server MUST reject a bundle whose IR version it cannot execute.

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

1. Decode request and enforce transport limits.
2. Authenticate the principal and construct `ActorContext`.
3. Resolve the active contract and command plan.
4. Validate input against the compiled schema.
5. Canonicalize the input and calculate its hash.
6. Derive partition and conflict keys from validated inputs.
7. Authorize the command, tenant, partition, and fields.
8. Derive the idempotency identity and ask the commit coordinator to create or resolve its durable pending reservation.
9. Return a terminal replay immediately, reject mismatched input, or resume the reserved contract version, plan, and `tx.time`.
10. Acquire all conflict keys in canonical order.
11. Recheck the reservation after lock acquisition.
12. Build a bounded materialized snapshot for every declared read and validation dependency.
13. Evaluate the deterministic command plan without storage I/O.
14. Construct the `riffdb-storage-api`-owned `CommitIntent` containing dependencies, mutations, events, outcome, and provenance.
15. Submit the intent to the commit coordinator.
16. Open a short coordinator-owned durable write transaction.
17. Recheck idempotency and versions and re-evaluate the exact predicate and invariant validation plan over transaction-current values plus proposed mutations.
18. Assign a `CommitSequence` and atomically write all terminal state.
19. Commit the storage transaction and release logical capabilities.
20. Notify commit subscribers, projection workers, and outbox workers.
21. Return the typed outcome envelope.

## 9.2 Transaction context

```rust
pub struct TransactionContext {
    pub request_id: RequestId,
    pub admitted_at: Timestamp,
    pub actor: ActorContext,
    pub contract_version: ContractVersion,
    pub plan_hash: PlanHash,
    pub partition_key: PartitionKey,
}
```

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
- Lock timeout returns an execution failure, not a declared business outcome, unless the contract explicitly maps timeout behavior.

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

`TXN-022` The runtime MUST emit lock-wait duration and conflict-key hash metrics without exposing raw business keys.

`TXN-023` Loom tests MUST cover grant, release, cancellation, timeout, fairness, and multi-key ordering for a reduced lock manager model.

## 9.4 Read dependencies

```rust
pub enum ReadDependency {
    EntityVersion {
        entity_type: EntityTypeId,
        key: EntityKey,
        expected_version: Option<u64>,
    },
    IndexRangeVersion {
        index: IndexId,
        prefix: Vec<u8>,
        expected_epoch: u64,
    },
    Predicate {
        invariant: InvariantId,
        captured_values: Vec<CanonicalValue>,
    },
}
```

POC commands SHOULD use primary-key reads. Bounded index reads are allowed only when the storage layer maintains a range or prefix epoch that can be revalidated. If this mechanism is not implemented in the first vertical slice, index reads that influence writes MUST fail compilation.

## 9.5 Commit intent

`riffdb-storage-api` owns the transport-neutral `CommitIntent` type. `riffdb-runtime` constructs an intent from the compiled plan and the bounded materialized snapshot supplied by application orchestration; the runtime does not open snapshots or transactions itself.

```rust
pub struct CommitIntent {
    pub request_id: RequestId,
    pub idempotency_identity: IdempotencyIdentity,
    pub input_hash: CanonicalInputHash,
    pub command_id: CommandId,
    pub contract_version: ContractVersion,
    pub plan_hash: PlanHash,
    pub actor: ActorContext,
    pub tx_time: Timestamp,
    pub partition_key: PartitionKey,
    pub conflict_key_hashes: Vec<ConflictKeyHash>,
    pub read_dependencies: Vec<ReadDependency>,
    pub mutations: Vec<EntityMutation>,
    pub durable_events: Vec<DurableEvent>,
    pub outcome: EncodedOutcome,
    pub provenance: ProvenanceInput,
}
```

`TXN-030` A `CommitIntent` MUST be fully self-contained and MUST NOT contain references into an API request buffer, parser AST, live storage transaction, or lock-manager internal state.

`TXN-031` Mutation and event ordering inside an intent MUST be canonical so hashing and replay are deterministic.

## 9.6 Commit coordinator

The commit coordinator runs on a dedicated blocking thread or tightly controlled blocking task because embedded storage writes are synchronous. API tasks submit intents through a bounded channel and await a one-shot response.

The commit coordinator is the only component permitted to create idempotency reservations, assign application commit sequences, or drive authoritative write transactions. It opens a write transaction only after deterministic command evaluation has completed. The write transaction is intentionally short: it rechecks identity and dependencies, supplies transaction-current values for the exact compiled validation plan, applies one bounded atomic record set, and commits. Runtime evaluation MUST NOT occur while a storage transaction is held. The storage API MUST NOT expose arbitrary transaction callbacks or a generic public write surface.

The coordinator performs:

1. Open durable write transaction.
2. Recheck idempotency key.
3. Revalidate read dependencies.
4. Re-evaluate commit-time invariant plans over current values plus proposed mutations.
5. Allocate next commit sequence.
6. Apply entity mutations and secondary index changes.
7. Persist command outcome and idempotency record.
8. Persist commit record and provenance.
9. Persist outbox entries.
10. Commit the storage transaction using configured durability.
11. Publish the committed result to the waiting caller and subscribers.

`TXN-040` The commit queue MUST be bounded and apply backpressure.

`TXN-041` The coordinator MUST NOT assign a visible sequence before the storage transaction can atomically persist that sequence and its complete record.

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
- A panic in command evaluation MUST be caught at the service boundary, recorded as an internal failure with a correlation ID, and release capabilities.
- The commit coordinator process should fail fast on an invariant breach indicating internal corruption rather than continue serving uncertain state.
- Recovery MUST run storage integrity checks and metadata consistency checks before readiness.

---

# 10. Durable storage design

## 10.1 Storage interface

The storage abstraction is semantic, not a generic database portability layer. It owns `ReadSnapshot`, `ReadDependency`, predicate-dependency representations, `CommitIntent`, committed outcome, durable record, and coordinator transaction types. The commit coordinator owns their orchestration and sequencing.

```rust
pub trait StorageEngine: Send + Sync + 'static {
    type Snapshot<'a>: ReadSnapshot
    where
        Self: 'a;
    type WriteTransaction<'a>: CoordinatorWriteTransaction
    where
        Self: 'a;

    fn snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError>;
    fn begin_coordinator_write(&self) -> Result<Self::WriteTransaction<'_>, StorageError>;
    fn get_outcome(&self, key: &IdempotencyLookup) -> Result<Option<StoredOutcome>, StorageError>;
    fn get_commit(&self, sequence: CommitSequence) -> Result<Option<CommitRecord>, StorageError>;
    fn scan_commits(
        &self,
        after: CommitSequence,
        limit: usize,
    ) -> Result<Vec<CommitRecord>, StorageError>;
}
```

`CoordinatorWriteTransaction` is a narrow typed interface for idempotency recheck, dependency reads, application of a coordinator-validated atomic record set, and commit or rollback. It does not assign sequences, interpret a `CommandPlan`, expose engine-native handles, or accept caller-provided closures. The exact method split is frozen by the storage semantic API ADR before WP-060 implementation.

The concrete `redb` adapter MAY contain additional private APIs for compaction, backup, integrity checks, and statistics.

## 10.2 Redb table layout

| Table | Key | Value |
|---|---|---|
| `meta` | UTF-8 system key | Versioned metadata value |
| `contract_bundles` | contract version, big endian | Contract bundle envelope |
| `catalog_active` | fixed key | Active contract version and hash |
| `entities` | entity type ID + canonical entity key | Entity record envelope |
| `secondary_indexes` | index ID + canonical index key + entity key | Empty marker or covered value |
| `index_epochs` | index ID + prefix bucket | Monotonic validation epoch |
| `idempotency` | canonical identity from Section 7.6 | Stored outcome pointer and input hash |
| `idempotency_pending` | canonical idempotency identity | Pending reservation, fixed transaction context, and plan identity |
| `commits` | commit sequence, big endian | Commit record envelope |
| `outbox` | commit sequence + event ordinal | Outbox entry |
| `outbox_status` | event ID | Delivery state and attempts |
| `projection_state` | projection ID + group key | Aggregate state |
| `projection_frontier` | projection ID | Frontier and lifecycle state |
| `capabilities` | opaque token hash | Capability record |
| `audit` | administration sequence | Security and administration event |

`STO-010` Ordered numeric keys MUST use big-endian encoding so lexicographic scans preserve numeric order.

`STO-011` Table names and key prefixes are storage-format API and require migration planning after POC format freeze.

`STO-012` The POC MUST maintain metadata keys for storage format version, database and node identity, next application commit sequence, next administration audit sequence, active contract, clean shutdown marker, and last successful integrity check.

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

## 10.4 Commit record

```protobuf
message CommitRecord {
  uint64 sequence = 1;
  bytes request_id = 2;
  uint32 command_id = 3;
  uint64 contract_version = 4;
  bytes plan_hash = 5;
  bytes canonical_input_hash = 6;
  Actor actor = 7;
  Timestamp tx_time = 8;
  repeated ReadDependency reads = 9;
  repeated EntityMutation mutations = 10;
  repeated DurableEvent events = 11;
  EncodedOutcome outcome = 12;
  Provenance provenance = 13;
  reserved 14; // replay is response metadata and never creates another commit
}
```

For POC rebuildability, entity mutations SHOULD include a complete post-image. Sensitive command input values MUST NOT be recorded by default; the record stores a canonical hash and selected policy-approved provenance fields.

## 10.5 Durability modes

| Mode | Behavior | Use |
|---|---|---|
| `sync` | Commit is acknowledged only after the embedded engine reports durable synchronization. | Default POC correctness mode |
| `group` | Coordinator batches compatible intents and performs one durable flush for the batch. | Benchmark and likely MVP default |
| `memory` | No durability guarantee. | Unit and model tests only; server refuses non-test startup |

The returned outcome MUST identify the durability mode used for its commit.

## 10.6 Recovery

Startup recovery performs:

1. Open the database and run engine integrity checks appropriate to configuration.
2. Validate storage format and node identity.
3. Verify active contract bundle and plan hash.
4. Verify `next_sequence` equals one greater than the last commit, repairing from the commit table if safe.
5. Verify idempotency pointers reference valid commit records.
6. Verify pending reservations are structurally valid and can be safely resumed or resolved by retry without allocating a sequence during recovery.
7. Verify projection frontier does not exceed the last commit.
8. Mark outbox entries in `Delivering` from the previous process as retryable according to policy.
9. Rebuild in-memory indexes, lock metrics, and subscriptions.
10. Set readiness only after validation succeeds.

`REC-001` Recovery MUST be idempotent.

`REC-002` Repeated restart after a crash MUST not create new commits, events, or outcome records.

`REC-003` Projection state MAY be discarded and rebuilt from commits; source entities and commit records MUST not depend on projection state for correctness.

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
- Every mutating call requires request ID, idempotency key, actor context, and expected contract version or explicit active-version behavior.
- The outer `request_id` identifies a transport submission. The caller's command `idempotency_key` is a separate typed command input and is the uncertainty-recovery identity component.

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
  uint64 expected_contract_version = 3;
  Value input = 4; // includes the contract-declared idempotency_key field
}

message ExecuteCommandResponse {
  enum CompletionStatus {
    COMPLETION_STATUS_UNSPECIFIED = 0;
    COMMITTED = 1;
    REPLAYED = 2;
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

The custom `Value` family is required; `google.protobuf.Struct` is forbidden at exact business-value boundaries because it cannot preserve all signed/unsigned integer and decimal values. Lists, records, strings, and bytes inherit compiled or protocol bounds. Record fields MUST be unique and canonically ordered by stable field ID where available, then by UTF-8 name. Decimal coefficient, scale, currency, UUID, date, and timestamp encodings MUST be validated and canonical before hashing or persistence.

`riffdb-proto` is the single owner of all checked-in `.proto` sources and generated compatibility fixtures. WP-020 freezes the package names, common exact `Value` messages, request/response envelopes, service and RPC names, bounds policy, generation toolchain, and durable envelope. Semantic records whose Rust owners are not yet stable, including final commit, capability, outbox, and projection payloads, are added later through small proto-owner interface PRs after the owning package has stabilized them. No other work package may create a competing schema or pre-emptively guess those fields.

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
- Automatic idempotent retry on safe transport failures using the same request and key.
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

`MCP-011` Transport-specific authentication MUST resolve to the same internal `ActorContext` and capability decision.

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
riffdb.cmd.<normalized_contract_name>.<normalized_command_name>
```

Example:

```text
riffdb.cmd.legal_spend.allocate_budget
```

- Contract and command segments use lowercase ASCII snake case (`[a-z][a-z0-9_]*`); dots separate the fixed prefix, contract segment, and command segment.
- Names MUST remain under 128 characters.
- Names MUST be unique in the active tool catalog.
- Deployment MUST reject any normalization collision. The server MUST NOT append an unstable or hash-derived suffix to make a collision appear valid.
- Contract deployment that adds, removes, or changes visible command tools emits `notifications/tools/list_changed` when negotiated.

### Generated tool definition

```json
{
  "name": "riffdb.cmd.legal_spend.allocate_budget",
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
| `MCP-046` | Record every mutating tool invocation and administrative read in the audit stream. |
| `MCP-047` | Stdio credentials MUST come from environment or protected local configuration, not command-line arguments visible in process listings. |
| `MCP-048` | HTTP deployment MUST reject tokens not audience-bound to the canonical MCP resource server. |
| `MCP-049` | The server MUST sanitize user-controlled strings included in Markdown or text results to prevent misleading instruction injection in generated operational summaries. |

For the POC HTTP transport, the protected resource identity is a configured canonical URI ending in `/mcp`; it MUST NOT be synthesized from the request `Host` header. The stdio bridge connects to `riffdbd` only through gRPC and therefore uses a capability explicitly carrying the configured gRPC audience. Tests MAY inject the API-neutral service in process, but production stdio has no in-process or storage path.

## 12.12 MCP conformance and compatibility

- Run the official MCP Inspector against stdio and Streamable HTTP variants.
- Add protocol initialization, tool listing, resource listing, schema, pagination, cancellation, and progress integration tests.
- Pin `rmcp` minor versions in the workspace lockfile.
- Maintain an internal adapter trait so a future MCP protocol revision can coexist during migration.
- The POC MUST report its supported protocol version and server implementation metadata accurately during initialization.

---

# 13. Authorization, capabilities, and provenance

## 13.1 Actor context

```rust
pub struct ActorContext {
    pub principal_id: ActorId,
    pub actor_kind: ActorKind,
    pub agent_session_id: Option<AgentSessionId>,
    pub source_repository: Option<String>,
    pub source_commit: Option<String>,
    pub reason: Option<String>,
    pub approval_id: Option<String>,
    pub capability_id: CapabilityId,
}
```

Fields supplied by clients are claims. The authorization layer decides which claims are trusted, required, stored, redacted, or ignored.

## 13.2 POC capability tokens

The POC uses opaque random bearer tokens backed by server-side capability records.

1. Generate exactly 32 bytes using operating-system cryptographic randomness outside the command runtime and encode them as unpadded base64url.
2. Return the encoded token once.
3. Store only an HMAC-SHA-256 digest with an explicit digest-key ID and version.
4. Resolve token to a capability record at request time.
5. Support expiry and revocation.

This avoids designing a new self-contained token format or cryptographic verification scheme for the POC.

```rust
pub struct CapabilityRecord {
    pub capability_id: CapabilityId,
    pub principal_id: ActorId,
    pub environment: Environment,
    pub database_id: DatabaseId,
    pub audiences: BTreeSet<Audience>,
    pub digest_key_id: DigestKeyId,
    pub digest_version: u32,
    pub expires_at: Timestamp,
    pub tenant_scope: TenantScope,
    pub command_allowlist: BTreeSet<CommandId>,
    pub entity_read_allowlist: BTreeSet<EntityTypeId>,
    pub field_policy: FieldPolicy,
    pub max_scan_rows: u32,
    pub allow_contract_validate: bool,
    pub allow_contract_deploy: bool,
    pub require_approval_for: BTreeSet<OperationClass>,
}
```

## 13.3 Authorization decision

Every operation yields a structured decision:

```rust
pub enum Decision {
    Allow { obligations: Vec<Obligation> },
    Deny { code: PolicyCode, safe_reason: String },
}
```

Obligations include field redaction, row limit, tenant predicate, approval requirement, audit level, and output classification.

`SEC-001` Authorization MUST be deny-by-default.

`SEC-002` A capability MUST be scoped to an environment and MUST NOT be accepted by another environment.

`SEC-003` Revocation MUST take effect on the next request; long-running operations recheck at defined safe points.

`SEC-004` Administrative deployment requires expected active version and an approval reference when policy requires it.

Capabilities MUST also be bound to the database/server identity and one or more explicit audiences. Capability creation, revocation, and catalog activation are authoritative typed administrative operations routed through the commit coordinator and recorded under the separate ordered administration audit sequence. No API or authentication component may write their storage tables directly.

The first local operator may be created only by a one-time bootstrap operation against an empty database. Bootstrap MUST fail closed once any bootstrap marker, capability, active catalog state, application commit, or administration record exists; its durable marker and capability creation are atomic. The exact HMAC key custody and rotation procedure requires the proposed capability-token ADR before WP-110 implementation.

## 13.4 Provenance

Every terminal admitted command stores:

- Principal and actor kind.
- Agent session ID.
- Request ID and idempotency key hash.
- Source repository and commit when validated or supplied as untrusted metadata.
- Contract version and plan hash.
- Command ID and canonical input hash.
- Partition and conflict-key hashes.
- Outcome type.
- Commit sequence.
- Affected entity identities and versions.
- Durable event identities.
- Approval and reason fields according to policy.

Provenance is immutable. Corrections are additional records linked to the original.

## 13.5 MVP authorization

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

The projection worker scans commit records in strictly increasing sequence. A projection advances its durable frontier only after all events from the next sequence are durably reflected in projection state.

```rust
pub struct ProjectionFrontier {
    pub projection_id: ProjectionId,
    pub applied_through: CommitSequence,
    pub lifecycle: ProjectionLifecycle,
    pub last_error: Option<SafeError>,
}
```

Lifecycle states:

```text
Building -> CatchingUp -> Ready
     |          |          |
     +----------+----------+-> Degraded -> Rebuilding -> Ready
                                  |
                                  +-> Invalid
```

`PRJ-001` A frontier MUST never decrease.

`PRJ-002` A frontier MUST never advance past a commit whose relevant events have not been durably applied.

`PRJ-003` Projection application MUST be idempotent by `(projection_id, commit_sequence)`.

`PRJ-004` Projection state MUST be rebuildable from the authoritative commit log.

## 15.3 Read-after-commit query

A query accepts optional `after_sequence` and wait deadline.

```rust
pub enum ProjectionQueryResult<T> {
    Ready {
        data: T,
        frontier: CommitSequence,
    },
    WaitTimedOut {
        required: CommitSequence,
        current: CommitSequence,
    },
    Degraded {
        current: CommitSequence,
        reason: String,
    },
    Invalid {
        reason: String,
    },
}
```

The server MUST NOT silently return stale data when `after_sequence` is provided and the frontier is behind.

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

```rust
pub struct HealthReport {
    pub status: HealthStatus,
    pub active_contract_version: Option<ContractVersion>,
    pub last_commit_sequence: CommitSequence,
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
| Outcome determinism | Same initial snapshot, normalized input, contract plan, and logical time produce the same outcome and `CommitIntent`; the POC command runtime has no randomness. |
| Idempotent replay | Repeating an admitted idempotency identity with equal canonical input never creates a second mutation or event set. |
| Idempotency mismatch rejection | Reusing an identity with a different canonical input never returns the earlier outcome as though it applied. |
| Conflict serialization | Histories sharing a mutable conflict key are observationally equivalent to an allowed serial order. |
| Lock release | Cancellation, business rejection, panic containment, and storage failure release all acquired capabilities. |
| Sequence monotonicity | Terminal admitted records have unique increasing sequences. |
| Recovery equivalence | Reopening after any completed durable prefix yields the same observable state as replaying that prefix in the model. |
| Projection prefix | A projection at frontier N equals model application of relevant events through N. |
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

## 17.5 Crash and failpoint matrix

The POC MUST support named, test-only failpoints. Process tests terminate the server rather than relying only on recoverable error returns.

| Failpoint | Required recovered state |
|---|---|
| Before command admission | No outcome, mutation, event, or sequence. |
| After pending idempotency reservation, before locking | Only the valid pending reservation is durable; retry reuses its contract version, plan, and `tx.time`; no sequence exists. |
| After lock acquisition, before evaluation | Pending reservation may remain; locks are absent after restart; no terminal state or sequence exists. |
| After evaluation, before commit submission | Pending reservation may remain; no mutation, event, outcome, or sequence is visible. |
| Before storage transaction commit | No partial mutation, outcome, event, or sequence. |
| During embedded-engine durable commit | Either entire atomic commit is visible or none, according to engine guarantee. |
| After durable commit, before coordinator response | Complete state visible; retry returns persisted result. |
| After coordinator response, before API response | Complete state visible; retry returns persisted result. |
| After outbox record commit, before dispatch | Event remains pending and is eventually retried. |
| After destination success, before delivery acknowledgment | Duplicate dispatch is allowed; connector/idempotency policy handles it. |
| After projection apply, before frontier commit | Replay is idempotent; frontier cannot skip required work. |
| After frontier commit, before query wakeup | Query observes persisted frontier after restart. |
| During contract deployment before active pointer switch | Previous bundle remains active. |
| After active pointer switch, before API acknowledgment | New bundle is active and deployment retry is resolved by expected version. |

`TEST-001` Every failpoint MUST have an automated assertion over entities, indexes, outcomes, commit records, outbox records, and projection frontier as applicable.

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

Use Criterion for component benchmarks and a separate process harness for end-to-end results. Every benchmark records:

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
2. Create a local operator capability and a restricted agent capability.
3. Validate and deploy the budget contract.
4. Verify the MCP client receives or can refresh the generated `allocate_budget` tool.
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

## 19.3 Work-package dependency graph

[Graphviz source — agent work-package dependency graph](diagrams/work_package_dag.dot)

The graph uses hard dependencies. Parallel work is encouraged only after shared types and interface contracts are merged.

## 19.4 POC work packages

| ID | Name | Depends on | Primary deliverable | Exit gate |
|---|---|---|---|---|
| `WP-000` | Repository foundation | None | Cargo workspace, toolchain, CI, ADR template, contribution rules, deterministic generation checks | Clean CI on skeleton workspace |
| `WP-010` | Core types and error taxonomy | WP-000 | IDs, versions, money/decimal type, time type, safe errors, canonical value model | Serialization and canonicalization properties pass |
| `WP-020` | Protobuf and durable envelope | WP-010 | Public/internal `.proto`, Protox/Prost build, versioned stored-record envelope | Golden compatibility fixtures pass |
| `WP-030` | Contract syntax | WP-010 | Logos lexer, LALRPOP grammar, source spans, AST | Parser corpus, diagnostics, fuzz smoke pass |
| `WP-040` | Typed IR and compiler | WP-030, WP-010 | Name resolution, type checker, invariant/outcome/dependency plans, JSON Schema | Budget contract compiles; invalid corpus rejects with stable diagnostics |
| `WP-045` | Budget comparison foundation | WP-040 | Shared workload/oracle and isolated PostgreSQL implementation | Deterministic oracle and PostgreSQL correctness preflight pass |
| `WP-050` | Contract catalog | WP-020, WP-040, WP-060 | Bundle lookup, catalog state semantics, compatibility report, and typed expected-version deployment operation for later coordinator routing | Catalog semantic and atomic-storage-operation tests pass; final routing evidence is WP-100/WP-120 |
| `WP-060` | Storage semantic API | WP-010, WP-020 | Narrow snapshot/commit/log-scan trait and in-memory reference implementation | Reference-model tests pass |
| `WP-070` | Redb storage engine | WP-020, WP-060 | Tables, coordinator write transaction, atomic records, indexes, recovery, integrity scan, backup | Storage properties and core process recovery matrix pass |
| `WP-075` | Fjall semantic comparison | WP-060, WP-070 | Isolated non-production Fjall adapter and unchanged conformance/benchmark harness | Conformance report and reproducible evidence pass |
| `WP-080` | Deterministic command runtime | WP-040, WP-060 | IR interpreter, fixed logical time, read dependencies, `CommitIntent`; no command randomness | Differential tests against model pass |
| `WP-090` | Conflict manager | WP-010 | Canonical multi-key exclusive acquisition, cancellation, metrics | Loom/Shuttle suites pass |
| `WP-100` | Commit coordinator and idempotency | WP-050, WP-070, WP-080, WP-090, WP-110 | Admission reservation, revalidation, sequence assignment, atomic outcome/event/provenance/outbox-intent commit, typed control-plane operations | Concurrency, core process crash, and lost-response replay tests pass |
| `WP-110` | Authorization and capability service | WP-010, WP-070 | Opaque capability records, policy decisions, obligations, and typed create/revoke operations for later coordinator routing | Policy, digest, revocation-state, and fail-closed tests pass; final non-bypass evidence is WP-120 |
| `WP-120` | API-neutral application service | WP-050, WP-100, WP-110 | Command, query, contract, commit, projection service interfaces | In-process end-to-end tests pass |
| `WP-125` | RiffDB comparison service adapter | WP-045, WP-120 | Budget workload adapter over the API-neutral service | Shared oracle passes against in-process RiffDB |
| `WP-130` | gRPC protocol and Rust SDK | WP-020, WP-120 | Tonic services, limits, auth interceptor, generated and ergonomic Rust client | Public API integration suite passes against injected composition |
| `WP-135` | Canonical comparison client | WP-125, WP-130 | Budget comparison through the public Rust SDK/gRPC path | Shared oracle passes through the canonical client path |
| `WP-140` | Native MCP interface | WP-040, WP-120, WP-130 | `rmcp` stdio-over-gRPC and HTTP adapters, dynamic tools/resources, schema results, progress/cancellation | MCP conformance and generated-tool tests pass |
| `WP-150` | CLI and local developer flow | WP-130 | `riffdb`, config, local token flow, JSON/human output | Acceptance steps executable without internal APIs |
| `WP-160` | Durable outbox | WP-100, WP-120 | Event scanner, delivery state, test connector, retry and duplicate simulations | Crash and duplicate-delivery tests pass |
| `WP-170` | Projection core | WP-100, WP-120 | Event consumers, count/sum state, durable frontier, read-after-sequence query | Prefix and restart properties pass |
| `WP-180` | Observability and diagnostics | WP-120 | Tracing, metrics, health, explain output, redaction | Required telemetry present without sensitive labels |
| `WP-185` | Final server composition | WP-130, WP-140, WP-160, WP-170, WP-180 | `riffdbd` wiring for gRPC, MCP HTTP, outbox, projection, observability, lifecycle, and health | Composed server integration and lifecycle tests pass |
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
| D | WP-040, WP-060 |
| E | WP-045, WP-050, WP-070, WP-080 |
| F | WP-075, WP-110 |
| G | WP-100 |
| H | WP-120 |
| I | WP-125, WP-130, WP-160, WP-170, WP-180 |
| J | WP-135, WP-140, WP-150 |
| K | WP-185 |
| L | WP-190 |
| M | WP-200 |

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
- Technical specification version 0.3
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

The long-lived budget comparison workload, deterministic oracle, and PostgreSQL baseline begin after the compiler as a non-production evidence track. They are not P0 gate members, but their public-adapter and benchmark evidence must be complete before WP-200 closes P2.

## 20.3 Stage P1 — durable standalone POC

**Objective:** Prove durable command-only mutation, concurrency safety, idempotent uncertainty recovery, provenance, outbox intent, and crash recovery.

**Deliverables:**

- Redb-backed state and commit records.
- Logical conflict manager and commit revalidation.
- Persistent idempotency outcomes.
- Contract catalog and atomic deployment.
- gRPC, Rust SDK, CLI.
- Atomic durable outbox intent as part of every applicable command commit; external dispatch remains P2.
- Core transaction-path process failpoints and integrity scan for redb and the commit coordinator.

**Gate P1:** All transaction-path and recovery properties pass under the budget acceptance workload and generated histories.

## 20.4 Stage P2 — native agent interface and projection proof

**Objective:** Demonstrate that agents can safely discover and invoke contracts as dynamic MCP tools and reason about durable outcomes and derived state.

**Deliverables:**

- Stdio and loopback Streamable HTTP MCP.
- Generated command tools with input/output schemas.
- Contract, command-plan, outcome, commit, provenance, projection, and health resources.
- Capability-filtered tool catalog and repeated authorization.
- External outbox dispatch with duplicate and recovery evidence.
- Projection filter/count/sum engine with a durable frontier.
- Final `riffdbd` composition and the full cross-component crash matrix.
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

Specification v0.3 records each ADR's current status. An Accepted record is
authoritative; a Proposed record remains planning input until its exact text
receives human review. Where this table and a work-package deadline differ, the
earlier deadline governs unless a reviewed reconciliation changes both sources.

| ADR | Status | Decision | Required before |
|---|---|---|---|
| `ADR-0001` | Proposed | Standalone database rather than PostgreSQL extension or control plane | P0 gate |
| `ADR-0002` | Accepted | Canonical bounded contract grammar, typed IR, versioning, and deterministic bundle | WP-030 grammar freeze and WP-040 interface |
| `ADR-0003` | Accepted | Logical pessimistic conflict ownership, dependency validation, and commit ordering | WP-060/WP-090 interfaces |
| `ADR-0004` | Proposed | Coordinator-driven semantic storage API and redb baseline | WP-060 interface |
| `ADR-0005` | Accepted | Idempotency identity, pending reservation, persisted outcomes, and sequence semantics | WP-060 key freeze and WP-100 |
| `ADR-0006` | Accepted | Phased single-owner Protobuf schemas, exact values, durable envelope, and compatibility policy | WP-020 schema implementation |
| `ADR-0007` | Proposed | Shared application service for gRPC, MCP, CLI, and SDK | WP-100 coordinator boundary and WP-120 interface |
| `ADR-0008` | Proposed | Native MCP qualified command names, resources, audiences, and stdio-over-gRPC model | WP-140 fixtures |
| `ADR-0009` | Proposed | Opaque HMAC-digested, environment/database/audience-bound POC capability tokens | WP-060 storage interface and WP-110 implementation |
| `ADR-0010` | Accepted | Event-derived POC projection engine and frontier semantics | WP-070 projection persistence and WP-170 |
| `ADR-0011` | Accepted | Canonical values, fixed-scale decimals, keys, serialization, and domain-separated hashing | WP-010 semantic types |
| `ADR-0012` | Proposed | Deterministic transaction context, durable logical time, and no POC command randomness | WP-080 implementation |
| `ADR-0013` | Accepted | Stable semantic IDs, canonical IR/bundle encoding, plan hashing, and schema generation | WP-040 interface |
| `ADR-0014` | Accepted | Projection-plan and contract-plan-root typed hash domains | WP-010 follow-up and WP-040 interface |
| `ADR-0015` | Accepted | Explicit binding-failure outcomes and command-only budget bootstrap | WP-030 follow-up and WP-040 fixtures |
| `ADR-0016` | Accepted | Canonical key components, typed partition identity, and complete index-key framing | WP-010 follow-up and WP-040 interface |

## 22.2 Decisions to resolve before implementation reaches the named gate

ADR-0015 resolved initial entity creation: the canonical `CreateBudget` compiled
command seeds the demo through the ordinary coordinator path, and direct
storage/admin seeding remains forbidden.

| Decision | Resolve by | Default in this specification |
|---|---|---|
| Are read-only commands stored in the outcome journal? | Before WP-100 exit | Only when idempotency or audit policy requires it; no mutation log record otherwise. |
| Can a command read multiple conflict domains while mutating one? | Before WP-080 exit | Yes if all mutable domains are declared up front and all influential reads are version-tracked. |
| How are index phantom dependencies represented? | Before indexed range reads enter POC | Indexed range reads are read-only in initial POC; write-influencing range predicates deferred or use explicit conflict keys. |
| How are deterministic runtime arithmetic and resource faults resolved after durable admission? | Before WP-080 implementation | No implementation default; ADR-0013 fixes that no `CommitIntent` or business outcome is produced, but durable admission, retry, abandonment, and public-error semantics require human approval. |
| Does the POC support contract removal of fields or commands? | Before WP-050 exit | Additive and documentation-only changes; destructive changes rejected. |
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
  "name": "riffdb.cmd.legal_spend.allocate_budget",
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
meta/last_commit_sequence
contract/bundle/<u64-be-version>
contract/active
entity/<entity-type-id>/<encoded-primary-key>
index/<index-id>/<encoded-index-key>/<encoded-primary-key>
idempotency/<database-id>/<environment>/<tenant-scope-hash>/<principal-id>/<contract-lineage>/<command-id>/<key-digest>
idempotency_pending/<canonical-idempotency-identity>
commit/<u64-be-sequence>
provenance/<provenance-id>
outbox/<event-id>
outbox_pending/<next-attempt-time>/<event-id>
projection_meta/<projection-id>
projection_state/<projection-id>/<encoded-group-key>
projection_applied/<projection-id>/<u64-be-sequence>
capability/<capability-id>
capability_token/<token-keyed-hash>
admin_audit/<u64-be-administration-sequence>
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

This appendix intentionally repeats the five canonical services from Section 11.2. A sixth entity or projection service, or abbreviated RPC aliases, is not part of v0.2. Public messages MUST use stable field numbers, reserve removed fields, bound nested sizes, use the exact `Value` family in Section 11.3, and distinguish absent values from defaults when semantics require it.

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
15. Proptest, Loom, Shuttle, Criterion, cargo-fuzz, and Insta documentation.
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
