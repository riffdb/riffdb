# RiffDB Architecture Decision Records

Architecture decision records (ADRs) document decisions that constrain RiffDB's
public, durable, security, or semantic boundaries. `AGENTS.md`, `SPEC.md`,
`work_packages.yaml`, and ADRs whose status is **Accepted** are authoritative.
A Proposed ADR is planning input only and cannot override those sources.

## Statuses

- **Proposed:** direction and exact wording are under review.
- **Accepted:** exact record text has received human approval and is authoritative.
- **Rejected:** the proposal will not be adopted; the record remains for history.
- **Superseded:** a later Accepted ADR replaces the decision; both records link to
  each other.

Changing a status to Accepted, Rejected, or Superseded requires explicit human
review. Implementation agents must not infer acceptance from an approved planning
direction, merged draft, or implementation choice.

## Index

| ADR | Title | Status |
|---|---|---|
| [0000](0000-template.md) | ADR template | Template |
| [0001](0001-standalone-database-boundary.md) | Standalone Database Boundary | Accepted |
| [0002](0002-bounded-typed-dsl-and-versioned-deterministic-ir.md) | Bounded Typed DSL and Deterministic Compilation Boundary | Accepted |
| [0003](0003-logical-conflict-dependency-validation-and-commit-ordering.md) | Logical Conflict Ownership, Dependency Validation, and Commit Ordering | Accepted |
| [0004](0004-semantic-storage-api-and-redb-baseline.md) | Semantic Storage API and Redb Baseline | Accepted |
| [0005](0005-idempotency-identity-terminal-outcomes-and-sequences.md) | Idempotency Identity, Terminal Outcomes, and Sequence Semantics | Accepted |
| [0006](0006-versioned-protobuf-and-durable-envelope.md) | Versioned Protobuf and Durable Envelope | Accepted |
| [0007](0007-shared-application-service-boundary.md) | Shared Application-Service Boundary | Accepted |
| [0008](0008-native-mcp-tool-and-resource-model.md) | Native MCP Tool and Resource Model | Proposed |
| [0009](0009-opaque-server-side-poc-capabilities.md) | Opaque Server-Side POC Capabilities | Accepted |
| [0010](0010-event-derived-projection-and-frontiers.md) | Event-Derived Projection and Frontier Semantics | Accepted |
| [0011](0011-canonical-values-keys-and-hashing.md) | Canonical Values, Fixed-Scale Decimals, Keys, and Hashing | Accepted |
| [0012](0012-deterministic-transaction-context.md) | Deterministic Transaction Context and Execution-Fault Admission | Accepted |
| [0013](0013-stable-ids-ir-encoding-and-plan-hash.md) | Stable IDs, IR Encoding, and Plan-Hash Framing | Accepted |
| [0014](0014-projection-and-contract-plan-hash-domains.md) | Projection and Contract Plan Hash Domains | Accepted |
| [0015](0015-explicit-binding-outcomes-and-budget-bootstrap.md) | Explicit Binding Outcomes and Budget Bootstrap | Accepted |
| [0016](0016-canonical-key-components-and-partition-identity.md) | Canonical Key Components and Partition Identity | Accepted |
| [0017](0017-projection-group-keys-generations-and-frontiers.md) | Projection Group Keys, Generations, and Durable Frontiers | Accepted |
| [0018](0018-uuidv7-generation-ownership-and-replay-boundaries.md) | UUIDv7 Generation, Ownership, and Replay Boundaries | Accepted |
| [0019](0019-poc-operational-metadata-deferral.md) | POC Operational Metadata Deferral | Accepted |
| [0020](0020-mcp-command-tool-name-normalization-and-compiler-ownership.md) | MCP Command Tool-Name Normalization and Compiler Ownership | Accepted |
| [0021](0021-service-audit-target-registry-and-canonical-ordering.md) | Service-Audit Target Registry and Canonical Ordering | Accepted |

The human architecture review on 2026-07-12 approved the direction represented
by ADR-0001 through ADR-0012. The human maintainer accepted ADR-0001 through
ADR-0007, ADR-0009 through ADR-0021, and their recorded companion amendments by
2026-07-13. ADR-0008 remains Proposed for resource URIs, HTTP audience,
stdio-over-gRPC transport, cursor presentation, and the remaining exact MCP
fixtures; ADR-0020 separately accepts command tool-name normalization and its
compiler/catalog ownership.

## Workflow

1. Copy `0000-template.md` to the next four-digit number.
2. Keep the record Proposed while options and consequences are reviewed.
3. Link requirements, work packages, fixtures, and superseded records explicitly.
4. Accept the exact text before a dependent package freezes a public or durable
   interface.
5. Amend an Accepted record with a new ADR when compatibility or semantics would
   change; do not silently rewrite history.
