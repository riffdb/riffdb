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
| [0001](0001-standalone-database-boundary.md) | Standalone Database Boundary | Proposed |
| [0002](0002-bounded-typed-dsl-and-versioned-deterministic-ir.md) | Bounded Typed DSL and Deterministic Compilation Boundary | Accepted |
| [0003](0003-logical-conflict-dependency-validation-and-commit-ordering.md) | Logical Conflict Ownership, Dependency Validation, and Commit Ordering | Proposed |
| [0004](0004-semantic-storage-api-and-redb-baseline.md) | Semantic Storage API and Redb Baseline | Proposed |
| [0005](0005-idempotency-identity-terminal-outcomes-and-sequences.md) | Idempotency Identity, Terminal Outcomes, and Sequence Semantics | Accepted |
| [0006](0006-versioned-protobuf-and-durable-envelope.md) | Versioned Protobuf and Durable Envelope | Accepted |
| [0007](0007-shared-application-service-boundary.md) | Shared Application-Service Boundary | Proposed |
| [0008](0008-native-mcp-tool-and-resource-model.md) | Native MCP Tool and Resource Model | Proposed |
| [0009](0009-opaque-server-side-poc-capabilities.md) | Opaque Server-Side POC Capabilities | Proposed |
| [0010](0010-event-derived-projection-and-frontiers.md) | Event-Derived Projection and Frontier Semantics | Proposed |
| [0011](0011-canonical-values-keys-and-hashing.md) | Canonical Values, Fixed-Scale Decimals, Keys, and Hashing | Accepted |
| [0012](0012-deterministic-transaction-context.md) | Deterministic Transaction Context | Proposed |
| [0013](0013-stable-ids-ir-encoding-and-plan-hash.md) | Stable IDs, IR Encoding, and Plan-Hash Framing | Proposed |

The human architecture review on 2026-07-12 approved the direction represented
by ADR-0001 through ADR-0012. ADR-0002, ADR-0005, ADR-0006, and ADR-0011 have
since received exact-text acceptance; every other record remains Proposed until
separately reviewed.

## Workflow

1. Copy `0000-template.md` to the next four-digit number.
2. Keep the record Proposed while options and consequences are reviewed.
3. Link requirements, work packages, fixtures, and superseded records explicitly.
4. Accept the exact text before a dependent package freezes a public or durable
   interface.
5. Amend an Accepted record with a new ADR when compatibility or semantics would
   change; do not silently rewrite history.
