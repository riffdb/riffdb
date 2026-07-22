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
| [0022](0022-durable-semantic-protobuf-schema-v1.md) | Durable Semantic Protobuf Schema Version 1 | Accepted |
| [0023](0023-wp100-coordinator-edge-semantics.md) | WP-100 Coordinator Edge Semantics | Accepted |
| [0024](0024-canonical-provenance-resource-locator.md) | Canonical Provenance Resource Locator | Accepted |
| [0025](0025-bootstrap-and-service-consumer-boundary-ownership.md) | Bootstrap and Service Consumer Boundary Ownership | Accepted |
| [0026](0026-wp120-authorization-recovery-and-failure-boundaries.md) | WP-120 Authorization, Recovery, and Failure Boundaries | Accepted |
| [0027](0027-service-response-budget-and-oversize-disposition.md) | Service Response Budget and Oversize Disposition | Accepted |
| [0028](0028-phase-zero-public-protobuf-completion.md) | Phase-Zero Public Protobuf Completion | Accepted |
| [0029](0029-two-stage-public-key-validation.md) | Two-Stage Public Key Validation | Accepted |
| [0030](0030-capability-partition-startup-evidence.md) | Capability Partition Startup Evidence | Accepted |
| [0031](0031-schema-directed-submitted-command-values.md) | Schema-Directed Submitted Command Values | Accepted |
| [0032](0032-same-session-retained-metadata-handoff.md) | Same-Session Retained Metadata Handoff | Accepted |
| [0033](0033-first-commit-notification-publication.md) | First-Commit Notification Publication | Accepted |
| [0034](0034-staged-application-service-activation.md) | Staged Application-Service Activation | Accepted |
| [0035](0035-atomic-authoritative-scan-fences.md) | Atomic Authoritative Scan Fences | Accepted |
| [0036](0036-schema-directed-submitted-query-components.md) | Schema-Directed Submitted Query Components | Accepted |
| [0037](0037-rust-sdk-foundational-type-dependencies.md) | Rust SDK Foundational Type Dependencies | Accepted |
| [0038](0038-partition-filtered-authoritative-index-scans.md) | Partition-Filtered Authoritative Index Scans | Accepted |
| [0039](0039-v2-index-migration-integration.md) | V2 Index Migration Integration | Proposed |
| [0040](0040-public-grpc-mcp-parity-bridge.md) | Public gRPC MCP Parity Bridge | Proposed |
| [0041](0041-cli-public-flow-and-output-contract.md) | CLI Public Flow and Output Contract | Proposed |

The human architecture review on 2026-07-12 approved the direction represented
by ADR-0001 through ADR-0012. The human maintainer accepted ADR-0001 through
ADR-0007, ADR-0009 through ADR-0038, and their recorded companion amendments by
2026-07-22. ADR-0008 remains Proposed for resource URIs other than the exact
provenance locator, HTTP audience, stdio-over-gRPC transport, cursor
presentation, and the remaining exact MCP fixtures; ADR-0020 separately accepts
command tool-name normalization and its compiler/catalog ownership, while
ADR-0024 accepts only the canonical provenance locator. ADR-0022 freezes the
exact WP-065 durable semantic-record field, tag, presence, and validation
registry. ADR-0023 freezes the coordinator edge semantics needed to join those
accepted interfaces. ADR-0025 freezes the auth-to-service bootstrap handoff,
service consumer ports, closed create invocation, and production-only bootstrap
construction required before WP-120. ADR-0026 freezes grammar-v1 operation
tenant scope, two-phase outcome-recovery authorization, storage-neutral
committed durability, and fail-closed incident-source failure required by
WP-120. ADR-0027 freezes API-neutral response accounting, whole-item byte-bound
pagination, and the closed oversize disposition. ADR-0028 freezes the complete
phase-zero public Protobuf inventory, field numbers, presence rules, and
structural-validation boundary before WP-127 implementation. ADR-0029 clarifies
that public protocol validation proves only the context-free key envelope while
the shared service owns schema-directed validation, including fail-closed
admission of explicit capability partition scopes. ADR-0030 assigns the shared
partition validator to the catalog and adds exact-end startup evidence for every
active, unexpired explicit capability scope without giving the server a
capability-enumeration bypass. ADR-0031 adds a service-owned pre-schema
submitted-value family so every transport remains structural while the service
materializes the sole canonical command input under the selected plan.
ADR-0032 carries the existing exact retained metadata in the completed
same-session structural handoff so WP-130 can select bootstrap lifecycle and
allocator readiness without a post-open storage bypass.
ADR-0033 supplies the missing sequence-only coordinator-to-server handoff for
first durable application commits while keeping catch-up, policy, redaction,
subscriber bounds, and public stream construction in the shared service/server
composition.
ADR-0034 freezes the two-stage Health-only application-service activation
boundary used while startup proofs are incomplete. ADR-0035 makes index and
commit scan fences atomic and represents the untouched index state explicitly
at storage, service, and public boundaries. ADR-0036 keeps public query
components structurally submitted until the shared service materializes them
against the selected schema. ADR-0037 permits the Rust SDK's direct,
default-feature-disabled dependency on the single owners of checked public
errors and foundational public identifiers while continuing to forbid every
authority-bearing server dependency. ADR-0038 adds exact partition identity to
durable index rows, a restartable offline V1-to-V2 migration, and bounded sparse
scan progress so explicit partition scopes reach storage without an unrestricted
post-filter or a policy dependency in the storage layer.

ADR-0039 remains Proposed for exact V2 descriptor isolation, codec-bound
migration evidence, historical-evidence replacement and ordering, corrective
package sequencing, linear startup migration, and narrow pre-sequence capacity
classification. ADR-0040 remains Proposed for the six-RPC public parity bridge,
conditional discovery, outcome-resource lookup, immutable operation-schema
catalog, and WP-137 gate placement. ADR-0041 remains Proposed for WP-150's exact
public-client dependency, credential, retry, configuration, and JSONL output
contract; it reserves but does not register the separately reviewed WP-155
backup/restore boundary. None of these proposals override current authority
until their exact text and companion reconciliations receive human acceptance.

## Workflow

1. Copy `0000-template.md` to the next four-digit number.
2. Keep the record Proposed while options and consequences are reviewed.
3. Link requirements, work packages, fixtures, and superseded records explicitly.
4. Accept the exact text before a dependent package freezes a public or durable
   interface.
5. Amend an Accepted record with a new ADR when compatibility or semantics would
   change; do not silently rewrite history.
