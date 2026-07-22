# ADR-0030: Capability Partition Startup Evidence

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21, amended 2026-07-22
- **Accepted:** 2026-07-21
- **Requires:** ADR-0004, ADR-0007, ADR-0009, ADR-0011, ADR-0016,
  ADR-0029
- **Amends:** ADR-0029 schema-validator ownership, startup reuse, and security
  disclosure
- **Amended by:** ADR-0039 for the distinct V2 index-migration startup evidence
  variant and its fixed ordering relative to capability evidence
- **Decision deadline:** Before WP-120 merges or WP-130 composes production
  readiness

The human maintainer accepted this exact record on 2026-07-21. It authorizes the
catalog-owned validator, exact-end startup evidence, coordinated corrective
paths, and security-disclosure correction below.

## Context

ADR-0029 correctly requires every untrusted explicit capability partition to
receive schema-directed validation before it can become authorization state. It
also requires production startup to revalidate every active, unexpired explicit
scope before readiness and says startup reuses a service-owned check.

The current accepted startup boundary cannot implement that last ownership
statement. `riffdb-storage-redb` scans every capability record while its ports
remain dormant, but the process-local `HistoricalSemanticEvidence` stream
contains only bundles, plan references, the active pointer, and persisted
entity/index keys. After the scan, `StructurallyOpened` contains only session
identity plus dormant ports. The operational `CapabilityReader` deliberately
offers point lookup by a caller-supplied ID and no unbounded enumeration.
`riffdb-service` currently has only a private create-path validator. WP-130 may
not add service, catalog, or storage interfaces and must not duplicate schema
logic in the server.

A new server-side capability enumeration port would expand post-open authority,
add a post-open scan of records already covered by the exclusive startup
snapshot, and require WP-130 to interpret records before readiness. Moving a
reusable pure validator into `riffdb-catalog` instead follows the existing
dependency direction: catalog is
already the sole IR-aware consumer of exact-end startup evidence, and the
service already depends on catalog.

ADR-0029 also overstates redaction. Its required validation-before-policy order
can reveal whether a candidate passed schema validation because a malformed
candidate returns `Validation` while a schema-valid unauthorized candidate may
return `AuthorizationDenied`. Static, redacted errors prevent disclosure of the
decoder reason or bytes, but cannot eliminate that one-bit validation oracle.

## Proposed Decision

### Shared schema validator

`riffdb-catalog` owns one pure, bounded capability-partition validator. It
accepts a `ValidatedContractBundle` and either one `ScopedPartitionV1` or a
complete `PartitionScopeV1`. For every explicit entry it:

1. requires the entry lineage to equal the bundle lineage;
2. reads the nonzero aggregate ID from the envelope-checked `PartitionKey`;
3. resolves the aggregate in the checked bundle;
4. selects that aggregate's exact partition `KeySchema`; and
5. invokes `decode_partition`, including complete-consumption validation.

The function returns one opaque, redaction-safe process-local error with no
lineage, owner, key bytes, component position, schema, enum, or decoder reason.
It is not a public protocol error, durable type, policy proof, or serializable
validation certificate.

`riffdb-service` continues to own normal-create preparation, cancellation,
deadlines, public error classification, authorization timing, audit ordering,
token issuance, capacity admission, and coordinator submission. It calls the
catalog-owned pure validator before the first policy evaluation exactly as
ADR-0029 requires. It does not retain a second decoder implementation or expose
a transport-facing validated-key type.

### Exact-end startup evidence

The storage API adds one process-local
`HistoricalCapabilityPartitionEvidenceV1` and one corresponding closed
`HistoricalSemanticEvidence` variant. One evidence value contains only:

- the structurally checked `CapabilityId` that owns the durable grant;
- the checked zero-based entry ordinal, bounded by the existing maximum of
  1,024 explicit partitions; and
- the existing `ScopedPartitionV1`, including its lineage and opaque complete
  partition-key envelope.

Its debug representation redacts the scoped partition. Its deterministic
evidence-order key uses top-level tag `0x05`, followed by the 16 capability-ID
bytes, a big-endian `u16` ordinal in `0..=1023`, a big-endian `u32` lineage
length and lineage bytes, the big-endian `u32` aggregate owner, a big-endian
`u32` key length, and the exact key bytes. Its semantic charge is exactly
`31 + lineage_length + key_length`, computed with checked arithmetic against
the existing evidence-page byte ceiling. This is process-local evidence framing
only; it is not persisted or hashed into a durable identity. Capability-ID byte
order has no priority, time, causality, or other semantic meaning.

During the same exclusive structural session, the redb and memory engines emit
exactly one such evidence item for every entry of every explicit partition scope
exactly when the owning capability has `CapabilityLifecycleV1::Active` and
`StartupValidationInputs.authorization_time < capability.expires_at()`. The
startup unexpired filter does not test `issued_at`: an `Active` record whose
authorization time is earlier than `issued_at` still emits evidence, matching
ADR-0009's existing startup readable-digest rule. Equality with or passage
beyond `expires_at` emits none. `PartitionScopeV1::All`, `Revoked` records, and
expired `Active` records emit no partition item; every capability record remains
subject to its existing structural, canonical, and reciprocal checks. Repeated
equal partition keys in different capabilities remain distinct through
capability ID and ordinal.

The evidence remains IR-opaque. Storage verifies the durable record, lifecycle,
exact expiry predicate, canonical scope, purpose/version/nonzero-owner
partition-key envelope, pagination, ordering, and exact end. The aggregate-owner
bytes in the evidence-order key are derived directly from that checked envelope;
there is no separately stored or evidenced owner field to compare. Storage does
not import contract IR or call a schema validator. A missing active catalog is
not repaired or substituted.

`riffdb-catalog` consumes the new evidence only after all `0x04` persisted-key
items that follow the active-pointer item in the canonical stream. It requires
an active validated bundle, applies the same pure validator used by the service,
and treats absence, lineage mismatch, unknown aggregate, malformed component
data, or incomplete consumption as `InvalidHistoricalEvidence`. The catalog
cannot produce `ValidatedCatalogHistory` until the complete ordered evidence
stream reaches its exact end.

The backend-private exact-end token is the authority that the complete source
namespace was enumerated, as already established by ADR-0004. Catalog inspection
of item payloads alone cannot prove that an entire durable capability record was
not omitted; adding a redundant per-item count would not prove that either.
Storage-engine conformance therefore compares emitted capability IDs and
ordinals with every qualifying durable record in the frozen snapshot. Catalog
tests independently reject every observable duplicate, reordering, malformed
item, and truncated or non-exact stream, but do not claim to infer a wholly
absent source row from the remaining payloads.

Consequently, WP-130 needs no capability enumeration port and no callable
schema validator. It mechanically combines the existing same-session
`StructurallyOpened` and `ValidatedCatalogHistory` values. A history value now
also proves that every active, unexpired explicit capability partition visible
in that exclusive startup snapshot was schema-valid against the active
compatible lineage.

### Security disclosure correction

ADR-0029's sentence claiming that generic failure prevents callers from probing
installed aggregate IDs or component schemas is replaced by this narrower
guarantee:

> Failures never echo the lineage, aggregate, key bytes, schema, component, enum,
> or decoder reason. Validation-before-policy can still reveal whether a
> candidate passed schema validation, so deployments must not treat aggregate or
> schema existence as secret.

The ordering, public `Validation` versus `AuthorizationDenied` classifications,
and fail-closed behavior remain unchanged.

## Options Considered

1. **Catalog-owned validator plus exact-end capability evidence:** Proposed. It
   reuses the existing exclusive snapshot and IR-aware evidence path without
   giving WP-130 a new record-enumeration authority.
2. **Public service validator plus a new startup enumeration port:** Rejected.
   It performs a second post-open scan, expands dormant/operational authority,
   and makes server composition interpret capability records.
3. **Duplicate the private validator in WP-130:** Rejected. It violates the
   shared-service and single-owner boundaries and can drift from runtime
   admission.
4. **Make storage decode `KeySchema`:** Rejected. It violates the IR-opaque
   storage boundary and would call semantic code from inside the engine's
   exclusive read transaction.
5. **Trust runtime admission and skip restart validation:** Rejected. Durable
   state is untrusted recovery input, and this would silently weaken ADR-0029's
   accepted readiness guarantee.
6. **Hide the validation oracle by changing ordering or error classes:**
   Rejected for this correction. Either change would alter accepted
   authorization and audit semantics and requires a separate decision.

## Consequences

- One shared decoder implementation serves normal creation and startup.
- `ValidatedCatalogHistory` gains a stronger process-local proof but no new
  fields, serialization, or durable compatibility burden.
- The storage API evidence enum gains a closed source-breaking variant before
  WP-130; memory, redb, catalog, fixtures, and exhaustive matches must change in
  one coordinated correction.
- Startup work grows with the number of active, unexpired explicit partition
  entries. Each entry and page is bounded by existing capability and evidence
  limits; no globally bounded capability count or total-startup-work claim is
  introduced.
- WP-130 remains a mechanical proof join and never receives raw capability
  enumeration authority.
- Aggregate and schema existence are not treated as confidential in the POC.

## Compatibility

This decision changes no Protobuf source, field number, wire bytes, storage key,
durable envelope, capability record, replay identity, digest, contract bundle,
key encoding, gRPC method, MCP schema, or public error code. No migration is
required.

`HistoricalSemanticEvidence` is a process-local closed Rust API, so adding the
variant is intentionally source-breaking for its exhaustive producers and
consumers. The change lands atomically across storage API, memory, redb,
catalog, and service before WP-130 begins. Compatibility fixtures freeze the
new evidence ordering and charge, not a persistent format.

Existing valid databases continue to open. A database containing an active,
unexpired explicit capability partition that never received schema validation
now correctly fails readiness as invalid historical evidence.

## Security

Capability partition bytes become policy evidence only after the shared catalog
validator succeeds. Runtime validation finishes before policy, token issuance,
capacity admission, or mutation. Startup validation finishes while storage
ports remain dormant and before `ValidatedCatalogHistory` can participate in
readiness.

Storage never sees IR, catalog never receives a token digest provider or
mutation port, service never receives startup enumeration authority, and server
composition never receives raw capability records. All diagnostics and debug
surfaces remain redacted. The limited validation oracle is documented rather
than hidden behind an unenforceable promise.

## Testing

The coordinated correction adds:

- storage-API unit tests for construction, 1,024-entry ordinal bounds,
  deterministic order/charge, and redacted debug output;
- memory/redb exact-end tests covering valid explicit entries, duplicate keys
  in different capabilities, `All`, active-expired, exact-expiry, future-issued
  `Active`, revoked, missing-active, page boundaries, continuation, and complete
  source enumeration against every qualifying durable record;
- catalog-history tests for valid keys and wrong lineage, absent aggregate,
  malformed component, trailing byte, missing active bundle, duplicate item,
  reordered item, and truncated or non-exact stream;
- one cross-implementation golden vector freezing the same capability ID,
  ordinal, scope, order key, and charge in storage API, memory, redb, and catalog;
- service tests for a mixed valid/invalid multi-entry scope and direct evidence
  that invalid preparation reaches neither policy, token issuer, control-plane
  reservation/submission, nor coordinator; and
- architecture tests proving storage remains IR-free, catalog does not depend on
  service, service has no storage dependency, and WP-130 receives only the
  existing opaque history proof.

No correctness test uses sleeps.

## Requirements and Work Packages

- **Requirements:** `API-001`, `SEC-001`, `STO-002`, `STO-012`, `REC-001`,
  `VAL-003`
- **Reopens for this correction:** WP-050, WP-060, and WP-070
- **Blocks:** WP-120 completion and WP-130 startup/readiness composition
- **Consumed by:** WP-127 stage-one key validation, WP-140, WP-150, WP-190, and
  WP-200
- **Final evidence:** WP-130 restart plus WP-190 recovery and WP-200 POC proof

### Coordinated correction ownership

This is one coordinated corrective change across already-started packages, not
WP-130 semantic work. WP-060 owns the storage-API evidence type and memory-engine
producer; WP-070 owns the redb producer; WP-050 owns the catalog validator and
historical-evidence consumer; WP-120 owns removal of the private validator and
reuse of the catalog-owned function. Acceptance of this ADR is the separate
interface and allowed-path approval required by AGENTS.md for only those edits
under `crates/riffdb-storage-api/**`, `crates/riffdb-storage-memory/**`,
`crates/riffdb-storage-redb/**`, `crates/riffdb-catalog/**`,
`crates/riffdb-service/**`, and their existing package test paths. It adds no
hard work-package dependency and transfers no other ownership. WP-130 remains a
proof join and receives no new semantic interface.

The correction must pass every affected package's existing acceptance commands:
`cargo test -p riffdb-storage-api -p riffdb-storage-memory`,
`cargo test -p riffdb-storage-redb`,
`cargo test -p riffdb-storage-redb --test storage_recovery_matrix`,
`cargo test -p riffdb-storage-redb --test service_audit_recovery`,
`cargo bench -p riffdb-storage-redb --no-run`, `cargo test -p riffdb-catalog`,
`cargo test -p riffdb-catalog --test contract_deploy_recovery`,
`cargo test -p riffdb-service`,
`cargo test -p riffdb-service --test service_end_to_end`,
`cargo test -p riffdb-service --test service_audit_orchestration`, and
`cargo deny check`, plus repository formatting and workspace Clippy checks.

## 2026-07-22 index-migration evidence amendment

ADR-0039 adds the distinct closed
`HistoricalSemanticEvidence::IndexMigrationRow` variant. For every physical V1
or V2 index row it replaces, and never accompanies, the former
`PersistedKey(IrOpaquePersistedKeyV1::IndexEntry)` evidence. Its exact order key
is byte-for-byte the former index-entry order key in the `0x04` domain. All
entity, index-range, and index-migration `0x04` evidence remains before every
capability-partition `0x05` item.

The initial exact-end pass consumes each checked migration row once and retains
only whether any V1 was observed. It retains no row evidence or migration
instruction. Only the bounded, linear, same-session migration rescan may produce
a fresh row and exactly one `V1Rewrite` or `V2Confirm` instruction. Missing,
duplicate, old-plus-new duplicate, reordered, wrong-discriminator, or any
`0x04`-after-`0x05` evidence fails closed. The existing capability evidence and
catalog-owned validation boundary otherwise remain unchanged.

## Decision Deadline

Exact acceptance is required before the private WP-120 decoder is treated as a
finished downstream contract or WP-130 begins production startup composition.
