# ADR-0115: Application Export Capability Successor

- **Status:** Proposed
- **Direction requested:** 2026-08-11
- **Decision deadline:** Before WP-575 adds any export request or durable export authority
- **Requires:** ADR-0006, ADR-0007, ADR-0009, ADR-0055, ADR-0089,
  ADR-0111, ADR-0112, and ADR-0114
- **Amends if accepted:** ADR-0089's closed capability successor chain
- **Defines or blocks:** WP-575, WP-578, and WP-579

## Context

ADR-0112 requires a distinct operator authority for whole-application export,
principal-filtered export under current row/field policy, and separately
authorized provenance and public-audit inclusion. None of the frozen durable
capability formats can express those rights. `CapabilityRecordV1`, migration
V2, installation V3, and row-policy V4 are schema-hash-bound compatibility
formats. Adding an enum member or field to any of them would make retained
capabilities unreadable or would reinterpret old authority.

Using `AdministerCapabilities`, `ReadEntity`, `ReadCommit`, or application-role
query permission as an export shortcut would violate the distinct-authority
rule and make a broad, resumable snapshot operation follow accidentally from a
different privilege. The export request also cannot carry a policy bypass or
authority claim.

## Proposed Decision

### A distinct V5 durable record

Keep V1 through V4 byte-for-byte unchanged. Add an own-file successor with the
following semantic shape:

```protobuf
enum CapabilityApplicationExportScopeV1 {
  CAPABILITY_APPLICATION_EXPORT_SCOPE_UNSPECIFIED = 0;
  CAPABILITY_APPLICATION_EXPORT_SCOPE_PRINCIPAL_FILTERED = 1;
  CAPABILITY_APPLICATION_EXPORT_SCOPE_WHOLE_APPLICATION = 2;
}

message CapabilityApplicationExportGrantV1 {
  string contract_lineage = 1;
  CapabilityApplicationExportScopeV1 scope = 2;
  bool entities = 3;
  bool events = 4;
  bool provenance = 5;
  bool public_audit = 6;
}

message CapabilityExportGrantExtensionV1 {
  repeated CapabilityApplicationExportGrantV1 applications = 1;
}

message CapabilityRecordV5 {
  CapabilityRecordV1 base = 1;
  CapabilityMigrationGrantExtensionV1 migration = 2;
  CapabilityInstallationGrantExtensionV1 installation = 3;
  CapabilityRowPolicyGrantExtensionV1 row_policy = 4;
  CapabilityExportGrantExtensionV1 export = 5;
}
```

V5 requires `base` and a nonempty `export` extension. Migration,
installation, and row policy remain optional with their exact existing
meanings. A capability without export authority continues to use the least
applicable V1 through V4 format. The export extension is authority in its own
right; it is not encoded as a new member of the frozen V1 permission enum.

The public capability-create/role-bind DTO gains one additive, typed export
grant field that lowers only into this extension. It is not an operation
request predicate. Public descriptor, fixture, CLI schema, and generated
binding rotation follows the accepted exact-identity ceremony.

### Canonical grant semantics

One V5 record carries one through 256 lineage grants ordered by canonical
lineage bytes, with no duplicate lineage. Each grant selects exactly one scope
and at least one of entities or events. Provenance and public audit are
independent explicit bits and never follow from either data-class bit.
Unknown scopes, false-only grants, duplicates, unordered grants, absent
lineages, and over-bound records are corrupt.

`PrincipalFiltered` requires an exact V4 row-policy extension, one matching
`ApplicationRoleIdentity` permission in the base grant, and current field
visibility. It exports only rows/events allowed by that current role and fact
set. `WholeApplication` is operator authority and does not require an
application role or row-policy binding; it still requires current capability,
global tenant scope, all-partition scope, exact database/audience, and the
named lineage grant. A principal-filtered grant can never request or delegate
whole-application scope.

Delegation may drop lineage grants, change whole-application to
principal-filtered only when the child also carries a valid narrowing V4 row
policy extension, drop data classes, or remove provenance/audit. It cannot add
a lineage/class, widen scope, or add optional protected classes.

The complete extension participates in the existing one-MiB capability
semantic bound, create/replay identity, revision, expiry, revocation, redacted
audit summary, and transaction-current checks. Diagnostics may name the
lineage, requested class, and required scope but never exported values,
principal facts, credentials, or hidden-row existence.

### Export safe points

Start, page, status, resume, and cancel each authenticate and reload the exact
current V5 capability. The immutable export receipt records capability ID and
revision, lineage, selected scope/classes, exact application/contract/module
identity, row-policy identity where applicable, and snapshot frontier. A
revision or authority change before a page is released closes the operation;
it never continues under the authority present at start.

Principal-filtered entity and event pages consume the same compiler-owned
policy proof as ordinary reads before serialization, counting, cursor
advancement, or hashes. Whole-application pages bypass principal row/field
policy only because the V5 grant explicitly says so; there is no request flag,
administrator implication, or storage-layer bypass.

Provenance and public-audit records are emitted only when their respective V5
bits are present. Private audit input, credentials, capability facts, internal
errors, storage identities, and redacted values remain unavailable even to a
whole-application export.

## Compatibility

V1, V2, V3, and V4 sources, descriptors, schema hashes, payload/envelope
goldens, and registry behavior remain exact. V5 receives its own FQN,
descriptor closure, schema hash, bounds, fixtures, registry tuple, and durable
format manifest entry. Current binaries read V1 through V5 and write the least
version required. Older binaries refuse V5 before mutation. There is no
down-conversion that silently removes export authority.

## Security

Default is deny. Export authority is not implied by administration, entity,
commit, role, backup, migration, or installation authority. Requests select
only a subset of the current V5 grant. Current capability revision and, for
principal-filtered scope, exact V4 facts/policies are revalidated before every
protected release. Server-owned output roots and bounded snapshot/cursor state
remain unchanged.

## Standing Design Tests

- **Interface safety:** no application/export request can express a policy
  bypass or synthesize export authority. Whole-application, provenance, and
  public-audit access each exist only as explicit current durable V5 facts.
- **Scale:** lineage grants are canonical and bounded; export remains paged,
  leased, resumable, snapshot-bound, and row/byte/time bounded.

## Testing

- Freeze every V1 through V4 source, descriptor, hash, payload, envelope, and
  registry literal before adding V5.
- Golden V5 fixtures cover principal-filtered, whole-application,
  provenance/audit, and combinations with migration/installation/row policy.
- Reject missing, empty, duplicate, unordered, unknown, over-bound, wrong-role,
  wrong-scope, stale-revision, and widening-delegation cases.
- Prove that administrator, backup, ordinary role, and entity-read credentials
  cannot start or observe an export without V5 authority.
- Race revocation, fact/policy/field narrowing, and scope changes before every
  page and terminal receipt.

## Acceptance

Human acceptance of this exact text is required before WP-575 changes a public
capability DTO, durable record, registry, or export authorization path.
