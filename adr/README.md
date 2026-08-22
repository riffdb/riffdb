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
| [0008](0008-native-mcp-tool-and-resource-model.md) | Native MCP Tool and Resource Model | Accepted |
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
| [0039](0039-v2-index-migration-integration.md) | V2 Index Migration Integration | Accepted |
| [0040](0040-public-grpc-mcp-parity-bridge.md) | Public gRPC MCP Parity Bridge | Accepted |
| [0041](0041-cli-public-flow-and-output-contract.md) | CLI Public Flow and Output Contract | Accepted |
| [0042](0042-sealed-catalog-owned-index-migration-driver.md) | Sealed Catalog-Owned Index Migration Driver | Accepted |
| [0043](0043-auth-owned-retained-mcp-credential.md) | Auth-Owned Retained MCP Credential | Accepted |
| [0044](0044-hosted-mcp-context-and-dependency-completion.md) | Hosted MCP Context and Dependency Completion | Accepted |
| [0045](0045-budget-safety-counterexample-evidence.md) | Budget Safety Counterexample Evidence and Claim Boundary | Accepted |
| [0046](0046-wp140-public-presentation-and-resource-conformance.md) | WP-140 Public Presentation and Resource Conformance | Accepted |
| [0047](0047-mcp-object-root-schema-conformance.md) | MCP Object-Root Schema Conformance | Accepted |
| [0048](0048-mcp-observer-physical-call-accounting.md) | MCP Observer Physical-Call Accounting | Accepted |
| [0049](0049-p2-derived-recovery-telemetry-and-hosted-composition.md) | P2 Derived Recovery, Telemetry, and Hosted Composition | Accepted |
| [0050](0050-public-offline-backup-and-restore-maintenance.md) | Public Offline Backup and Restore Maintenance | Accepted |
| [0051](0051-riffql-bounded-symbolic-query-language.md) | RiffQL Bounded Symbolic Query Language | Accepted |
| [0052](0052-versioned-query-modules-and-application-surfaces.md) | Versioned Query Modules and Application Surfaces | Accepted |
| [0053](0053-composite-query-snapshots-and-current-view-publication.md) | Composite Query Snapshots and Current-View Publication | Accepted |
| [0054](0054-bounded-dependent-key-batches.md) | Bounded Dependent Key Batches | Accepted |
| [0055](0055-safe-application-surface-and-declared-integrity.md) | Safe Application Surface and Declared Integrity | Accepted |
| [0056](0056-agent-application-alpha.md) | Agent Application Alpha Before Operational and Distributed Alpha | Accepted |
| [0057](0057-compiler-owned-application-lock-and-alpha-recovery.md) | Compiler-Owned Application Lock and Agent-Alpha Recovery | Accepted |
| [0058](0058-bounded-group-durability-and-audited-command-transitions.md) | Bounded Group Durability and Audited Command Transitions | Accepted |
| [0059](0059-same-partition-read-dependencies-and-write-parity.md) | Same-Partition Read Dependencies and Application Write Parity | Accepted |
| [0060](0060-bounded-scheduler-storage-lanes-and-parallel-preparation.md) | Bounded Scheduler Windows, Activated Storage Lanes, and Parallel Preparation | Accepted |
| [0061](0061-semantic-durability-terminal-admission-and-storage-v2.md) | Semantic Durability, Terminal Admission, and Storage Format V2 | Accepted |
| [0062](0062-generic-deployment-required-capability-administration.md) | Generic Deployment-Required Capability Administration | Accepted |
| [0063](0063-bounded-multiple-databases-per-process.md) | Bounded Multiple Databases Per Server Process | Accepted |
| [0064](0064-underscore-mcp-tool-names-and-agent-presentation.md) | Underscore MCP Tool Names and Agent Presentation | Accepted |
| [0065](0065-first-real-application-experience.md) | First Real Application Experience | Accepted |
| [0066](0066-additive-contract-evolution-and-deployment-diagnostics.md) | Additive Contract Evolution and Deployment Diagnostics | Accepted |
| [0070](0070-read-stability-and-internal-retry.md) | Read Stability and Internal Retry | Accepted |
| [0071](0071-typed-saturation-and-admission.md) | Typed Saturation and Admission | Accepted |
| [0072](0072-database-history-incarnation.md) | Database History Incarnation | Accepted |
| [0073](0073-linear-startup-validation-and-bounded-history-access.md) | Linear Startup Validation and Bounded History Access | Accepted |
| [0074](0074-python-application-driver.md) | Rust-Backed Python Application Driver | Accepted |
| [0075](0075-successor-application-identity.md) | Exact Successor Application Identity Before Mutation | Accepted |
| [0076](0076-offline-contract-migration-semantics.md) | Offline Contract Migration Semantics and Cutover | Accepted |
| [0077](0077-migration-language-ir-and-application-lock.md) | Migration Language, IR, and Application Lock | Accepted |
| [0078](0078-staged-migration-storage-and-recovery.md) | Staged Migration Storage, Publication, and Recovery | Accepted |
| [0079](0079-public-contract-migration-administration.md) | Public Contract Migration Administration | Accepted |
| [0080](0080-partitioned-events-consumers-and-live-queries.md) | Partitioned Events, Durable Consumers, and Live Queries | Accepted |
| [0081](0081-supported-migration-parent-and-canonical-successor.md) | Supported Migration Parent and Canonical Successor | Accepted |
| [0082](0082-single-total-order-and-pre-alpha-format-acceptances.md) | Single Total Order and Pre-Alpha Format Acceptances | Accepted |
| [0083](0083-authoritative-entity-references.md) | Authoritative Entity References | Accepted |
| [0084](0084-batch-item-results.md) | Batch Item Results | Accepted |
| [0085](0085-history-retention-and-startup-scaling.md) | History Retention and Startup Scaling | Accepted |
| [0086](0086-columnar-projection-and-read-freshness-classes.md) | Columnar Projection, Read Sources, and Freshness Policies | Accepted |
| [0087](0087-projected-ad-hoc-query-surface.md) | Projected Ad-Hoc Query Surface and Resource Governance | Accepted |
| [0088](0088-contract-migration-check-receipt.md) | Contract Migration Check Receipt | Accepted |
| [0089](0089-additive-migration-capability-record.md) | Additive Migration Capability Record | Accepted |
| [0090](0090-stable-id-rename-aliases.md) | Stable-ID Rename Aliases | Accepted |
| [0091](0091-native-vector-search-projections.md) | Native Vector Search as a Projection | Accepted |
| [0092](0092-native-full-text-search-projections.md) | Native Full-Text Search as a Projection | Accepted |
| [0093](0093-replicated-availability-changelog-shipping.md) | Replicated Availability via Authoritative Changelog Shipping | Accepted |
| [0094](0094-compiler-proved-commutative-child-append-groups.md) | Compiler-Proved Commutative Child-Append Groups | Accepted |
| [0095](0095-transaction-local-serial-command-micro-batches.md) | Transaction-Local Serial Command Micro-Batches | Accepted |
| [0096](0096-dynamic-groups-under-static-safety-ceiling.md) | Dynamic Physical Groups Under a Static Safety Ceiling | Accepted |
| [0097](0097-bounded-generated-batch-writer-feeding.md) | Bounded Generated-Batch Writer Feeding | Accepted |
| [0098](0098-bounded-redb-durability-epochs.md) | Bounded Redb Durability Epochs | Accepted |
| [0099](0099-canonical-command-capsules.md) | Canonical Command Capsules and Locator Rows | Accepted |
| [0100](0100-changelog-frames-follow-published-durable-frontiers.md) | Changelog Frames Follow Published Durable Frontiers | Proposed |
| [0101](0101-pipelined-standard-durability-journal.md) | Pipelined Standard Durability Journal | Accepted |
| [0102](0102-segmented-command-authority-and-derived-locators.md) | Segmented Command Authority and Rebuildable Exact Locators | Accepted |
| [0103](0103-preallocated-recyclable-durability-journal.md) | Preallocated Recyclable Durability Journal | Accepted |
| [0104](0104-journal-authoritative-state-overlay.md) | Journal-Authoritative Published State Overlay | Accepted |
| [0105](0105-authenticated-remote-application-ingress.md) | Authenticated Remote Application Ingress | Proposed |
| [0106](0106-rust-owned-multilanguage-driver-platform.md) | Rust-Owned Multilanguage Driver Platform | Proposed |
| [0107](0107-compiler-bounded-collection-mutations.md) | Compiler-Bounded Collection Mutations | Proposed |
| [0108](0108-bounded-operational-riffql.md) | Bounded Operational RiffQL and Safe Catalog Introspection | Proposed |
| [0109](0109-compiled-workflow-concurrency.md) | Compiled Workflow Concurrency and Fenced Leases | Proposed |
| [0110](0110-application-installation-and-adapter-conformance.md) | Exact Application Installation and Adapter Conformance | Proposed |
| [0111](0111-compiled-principal-row-policies.md) | Compiled Principal-Aware Row Policies | Accepted |
| [0112](0112-alpha-format-compatibility-and-application-portability.md) | Alpha Format Compatibility and Application Portability | Accepted |
| [0113](0113-deterministic-simulation-testing.md) | Deterministic Simulation Testing for the Durable Engine | Accepted |
| [0114](0114-row-policy-capability-successor.md) | Row-Policy Capability Successor | Accepted |
| [0115](0115-application-export-capability-successor.md) | Application Export Capability Successor | Accepted |
| [0116](0116-current-row-event-policy-anchors.md) | Current-Row Policy Anchors for Durable Events | Accepted |
| [0117](0117-compiled-external-framework-profiles.md) | Compiled External-Framework Profiles | Accepted |
| [0118](0118-secret-field-classification.md) | Secret Field Classification and Display-Surface Redaction | Accepted |
| [0119](0119-compiler-owned-workflow-reconstitution.md) | Compiler-Owned Workflow Reconstitution for Portable Reimport | Accepted |
| [0120](0120-driver-resident-developer-experience.md) | Driver-Resident Developer Experience | Accepted |
| [0121](0121-empty-project-schema-and-selected-binding-materialization.md) | Empty Project Schema and Selected Binding Materialization | Accepted |
| [0122](0122-agent-rails-and-application-guidance-resource.md) | Agent Rails and Application Guidance Resource | Accepted |
| [0123](0123-portable-cloud-performance-and-compact-writer-frames.md) | Portable Cloud Performance and Compact Command Segments | Accepted |
| [0124](0124-version-topology-and-retirement-governance.md) | Version Topology and Retirement Governance | Accepted |
| [0125](0125-command-segment-raw-fallback-compatibility.md) | Command-Segment Raw Fallback Compatibility | Accepted |
| [0126](0126-compiler-bounded-one-hop-cascade-deletion.md) | Compiler-Bounded One-Hop Cascade Deletion | Accepted |
| [0127](0127-bounded-multiplexed-application-session.md) | Bounded Multiplexed Application Session | Accepted |
| [0128](0128-compiler-declared-secret-outputs-for-named-riffql.md) | Compiler-Declared Secret Outputs for Named RiffQL | Accepted |
| [0129](0129-bounded-prepared-command-finalization.md) | Bounded Prepared Command Finalization and Pay-Once Batch Apply | Accepted |
| [0130](0130-compiler-planned-projection-result-sets.md) | Compiler-Planned Projection Result Sets and Snapshot-Aligned Composition | Accepted |
| [0131](0131-exact-indexed-text-cardinality-and-ordinal-windowing.md) | Exact Indexed Text Matching, Cardinality, and Ordinal Windowing | Accepted |
| [0132](0132-bounded-journal-fence-pipeline.md) | Bounded Journal Fence Pipeline and Ordered Durable-Prefix Publication | Accepted |
| [0133](0133-compiler-planned-covering-result-batches.md) | Compiler-Planned Covering Result Batches and Compact Named-Query Carriage | Accepted |
| [0134](0134-compiler-declared-exact-predicate-and-order-families.md) | Compiler-Declared Exact Predicate and Independent Order Families | Accepted |
| [0135](0135-packed-compiled-result-carriage.md) | Packed Compiler-Bound Named-Result Carriage | Accepted |
| [0136](0136-authoritative-vector-evidence-and-projected-nearest.md) | Authoritative Vector Evidence and Production Projected Nearest | Accepted |
| [0137](0137-bounded-framed-application-transport.md) | Bounded Framed Application Transport | Proposed |

The human architecture review on 2026-07-12 approved the direction represented
by ADR-0001 through ADR-0012. The human maintainer accepted ADR-0001 through
ADR-0042 and their recorded companion amendments by 2026-07-22, and accepted
ADR-0043 through ADR-0048 on 2026-07-23, then ADR-0049 and ADR-0050
on 2026-07-24. ADR-0008 now
accepts the native MCP tool/resource model, policy-filtered HTTP and stdio-over-
gRPC boundaries, canonical locators/cursors, and presentation rules; ADR-0020
separately owns command tool-name normalization and its compiler/catalog
boundary, while ADR-0024 owns the canonical provenance locator. ADR-0022 freezes the
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

ADR-0039 accepts exact V2 descriptor isolation, codec-bound migration evidence,
historical-evidence replacement and ordering, corrective package sequencing,
linear startup migration, and the narrow pre-sequence aggregate-capacity
classification. ADR-0040 accepts the six-RPC public parity bridge, conditional
discovery, outcome-resource lookup, immutable operation-schema catalog, and
WP-137 gate placement. ADR-0041 accepts WP-150's exact public-client dependency,
credential, retry, configuration, and JSONL output contract; at that checkpoint
it reserved but did not register WP-155. ADR-0050 now resolves that separately
reviewed backup/restore boundary.
ADR-0042 seals catalog-owned backend-branded migration instructions, progress,
and completion; narrows storage API to evidence and identity-only contracts;
and permits memory/redb's exact migration-only catalog edge with server-hidden
backend helpers.
ADR-0043 adds one auth-owned non-cloneable, nonserializable, redacted,
zeroizing 43-byte retained opaque credential helper for WP-140. Streamable HTTP
may only bound and copy bearer-stripped bytes into that helper and borrow
`OpaqueCredential` for the unchanged `CredentialAuthenticator`; it receives no
token parser, authentication decision, session credential cache, or new
dependency owner.
ADR-0044 adds the closed service-owned hosted-MCP `RequestContext` constructor,
the foundational types/errors and narrow tracing owners required by the common
adapter, the current-thread stdio runtime, and the non-reloadable rmcp telemetry
filter. It adds no policy edge, request-context claim carrier, transport
authority, wire field, or durable field.
ADR-0045 adds one isolated, non-gating WP-139 budget-safety evidence track with
four exact PostgreSQL negative controls and public-RiffDB contrasts, a bounded
versioned report/runner, a strict claim qualifier, and a WP-200 dependency. It
changes no product binary, production dependency, contract, public protocol,
durable format, storage boundary, or P0/P1/P2 membership.
ADR-0046 corrects the WP-137/WP-140 public presentation boundary with additive
decimal precision and bounded contract-compatibility metadata, service-owned
schema-bound outcome names, exact name-only enum submission, factual generated
command documentation, complete resource content, and hosted-only
post-authentication MCP limiting. It changes no durable, IR, bundle, plan-hash,
storage, URI, RPC, or dependency boundary.
ADR-0047 makes one pre-release in-place correction to thirteen canonical MCP
schema documents by adding the required redundant object-root type, with no
instance-shape, converter, compiler, public Protobuf, or durable-format change.
ADR-0048 separates fourteen-per-tick logical observer operations from the
physical service calls they contain, enforces exact one/five-call operation
handles and a 46-per-tick/8,280-per-session physical ceiling, and removes the
redundant stdio parity probe.
ADR-0049 completes P2's derived-worker and process-composition boundaries:
bounded exact-end outbox recovery observations, atomic internal projection
fences, catalog-owned event materialization, narrow projection expression
execution, closed owner telemetry before arbitrary subscribers, and optional
loopback hosted MCP with exact audience and lifecycle behavior.
ADR-0050 activates WP-155 after WP-185 with exactly three public offline
maintenance operations, current and staged authorization, one external
checksummed receipt ledger, exclusive drain/offline/validation lifecycle, and
the explicit DatabaseId-preserving restore-rewind limitation.

The earlier four records ADR-0008, ADR-0039, ADR-0040, and ADR-0041 cite
revision `21a8cfb` as their acceptance reference. Their separately named future exact-byte checkpoints
remain mandatory: WP-137 operation-schema source, identity, and composition
bytes; WP-140 fixed-tool-schema and resource-registry bytes; and WP-150 command
grammar, versioned DTO, JSONL, and stderr goldens. Acceptance does not
waive separate review of any differing real or resolved dependency lock graph,
feature set, unsafe inventory, or cryptographic or native edge. At that earlier
checkpoint it did not pre-accept WP-155; ADR-0050 now separately activates the
exact reviewed boundary.

ADR-0042 has a separate acceptance reference: the human maintainer's explicit
confirmation in the current Codex session on 2026-07-22. It does not claim the
earlier four records' `21a8cfb` acceptance reference.
ADR-0043 likewise has a separate acceptance reference: the human maintainer's
explicit confirmation in the current Codex session on 2026-07-23.
ADR-0044 likewise has a separate acceptance reference: the human maintainer's
explicit confirmation in the current Codex session on 2026-07-23.
ADR-0045 likewise has a separate acceptance reference: the human maintainer's
explicit confirmation in the current Codex session on 2026-07-23.
ADR-0046 likewise has a separate acceptance reference: the human maintainer's
explicit confirmation in the current Codex session on 2026-07-23.
ADR-0047 likewise has a separate acceptance reference: the human maintainer's
explicit confirmation in the current Codex session on 2026-07-23.
ADR-0048 likewise has a separate acceptance reference: the human maintainer's
explicit confirmation in the current Codex session on 2026-07-23.
ADR-0049 likewise has a separate acceptance reference: the human maintainer's
explicit confirmation in the current Codex session on 2026-07-24.
ADR-0050 likewise has a separate acceptance reference: the human maintainer's
explicit confirmation in the current Codex session on 2026-07-24.
ADR-0051 through ADR-0053 likewise have a separate acceptance reference: the
human maintainer's explicit 2026-07-28 authorization in the current Codex
session to make the implementation decisions required through WP-270. Together
they establish bounded symbolic RiffQL, immutable exact-contract query modules,
the additive application API hierarchy, one-snapshot composite execution,
rebuildable current catalog/capability views, and a unary-gRPC-first performance
strategy.
ADR-0054 has a separate acceptance reference: the human maintainer's explicit
2026-07-29 direction in the current Codex session to implement WP-275 and close
the TicketDesk POC gap. It adds only bounded collection-to-complete-key
dependencies and one-snapshot dependent point batches; it does not admit
general SQL or arbitrary joins.
ADR-0055 has a separate acceptance reference: the human maintainer's explicit
2026-07-29 agreement in the current Codex session with the safe-application
product rule. It separates named application, ad-hoc agent, and kernel
authority; requires whole-query fuel plus declared relationship and uniqueness
integrity; and adds a negative-canary gate without removing the compatible
kernel protocol.
ADR-0056 has a separate acceptance reference: the maintainer's explicit
2026-07-29 direction in the current Codex session to build the complete planned
Agent Application Alpha phase. It places application-authoring alpha before
operational/distributed alpha and freezes the boundary described by the exact
record text while retaining separate interface-fixture reviews.
ADR-0057 has a separate acceptance reference: the maintainer's explicit
2026-07-29 approval of the exact compiler-owned lock and failed-campaign
remediation record.
ADR-0058 has a separate acceptance reference: the maintainer's explicit
2026-07-29 acceptance of the exact bounded group-durability, two-transition
audited command lifecycle, independent uncertainty, and redb `Immediate`
durability amendment.
ADR-0059 has a separate acceptance reference: the maintainer's explicit
2026-07-29 acceptance of same-partition external reads, one compiler-proven
mutation aggregate, transaction-current dependency validation, and the
PostgreSQL write-parity gate, plus the 2026-07-30 approval of 64-command
internal grouping.
ADR-0060 has a separate acceptance reference: the maintainer's explicit
2026-07-30 approval of the bounded scheduler-window, admission-selection,
activated storage-lane, immutable-artifact, parallel-preparation, and
concurrent application-parity plan while retaining one ordered authoritative
writer and every fail-closed command guarantee.
ADR-0061 has a separate acceptance reference: the maintainer's explicit
2026-07-30 approval of the pre-alpha semantic durability contract, standard
one-phase and hardened two-phase redb profiles, atomic terminal admission,
compact storage format V2, single authoritative event payload, conservative
partition/index generations, and the prohibition on visible non-durable
chaining.
ADR-0062 has a separate acceptance reference: the maintainer's explicit
2026-07-30 approval of authenticated capability creation, revocation, and
policy-filtered command/resource discovery during `DeploymentRequired` through
the unchanged service, policy, audit, and coordinator paths. It permits a
generic empty database to create a distinct application-author identity and
discover fixed contract-authoring surfaces without deploying a bundled example
or admitting any application data operation.
ADR-0063 has a separate acceptance reference: the maintainer's explicit
2026-07-30 approval of at most 32 configured, independently durable databases
per process; selector-before-authentication routing; compatibility for one
implicit `default` database; unchanged v1 locators and durable bytes; and no
cross-database operation or runtime database administration.
ADR-0064 has a separate acceptance reference: the maintainer's explicit
2026-07-30 approval of the incompatible pre-alpha underscore-only MCP tool-name
cut, with no dotted aliases, plus bounded actionable input diagnostics,
agent-facing generated command documentation, and an agent cookbook.
ADR-0065 has a separate acceptance reference: the maintainer's explicit
2026-07-30 approval of the complete First Real Application Experience plan,
including journaled offline database addition for user and system installs,
schema-directed natural JSON, lock-only resumable deployment, explicit
least-authority role provisioning, application MCP named-query tools, selected
database identity on public responses, and the installed legacy-to-multi
acceptance gate.
ADR-0066 has a separate acceptance reference: the maintainer's explicit
2026-07-30 approval of bounded additive enum, entity, aggregate, relationship,
and uniqueness evolution; explicit-version enum-variant additions; structured
deployment diagnostics; and an operator-controlled pre-alpha dogfood reset.

## Workflow

1. Copy `0000-template.md` to the next four-digit number.
2. Keep the record Proposed while options and consequences are reviewed.
3. Link requirements, work packages, fixtures, and superseded records explicitly.
4. Accept the exact text before a dependent package freezes a public or durable
   interface.
5. Amend an Accepted record with a new ADR when compatibility or semantics would
   change; do not silently rewrite history.
