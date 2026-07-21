# ADR-0029: Two-Stage Public Key Validation

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Requires:** ADR-0007, ADR-0009, ADR-0011, ADR-0016, and ADR-0028
- **Clarifies:** Context-free public key-envelope validation, schema-directed
  service validation, and explicit capability partition-scope admission
- **Decision deadline:** Before WP-120 merges its capability-create service
  semantics or WP-127 freezes public key validators and fixtures

The human maintainer accepted this exact record on 2026-07-21. It authorizes
the two-stage implementation boundary and the fail-closed explicit capability
partition-scope rules below.

## Context

Accepted ADR-0011 deliberately splits typed-key validation. `riffdb-types`
checks purpose, key-codec version, nonzero owner identity, minimum envelope
length, and the 4 KiB complete-key bound. It explicitly states that this check
cannot claim component-schema validation. Accepted ADR-0016 explains why: v1
key component payloads carry no redundant type tags, so exact component count,
types, declared bounds, enum membership, and full consumption can be checked
only with the exact compiler-produced `KeySchema` in `riffdb-contract-ir`.

Accepted ADR-0028 nevertheless says that WP-127's context-free public protocol
validators fully decode entity, index-entry, and partition keys through their
semantic key codecs. `riffdb-proto` is forbidden from depending on the catalog,
contract IR, or service. It therefore cannot select the required `KeySchema`,
and an envelope-only check cannot satisfy that wording. Making WP-130's gRPC
adapter query the catalog would instead violate accepted ADR-0007's API-neutral
service boundary and make transport conversion semantic.

The same split exposes a concrete authorization issue. ADR-0009 defines an
explicit capability partition entry as one lineage plus one complete validated
`PartitionKey`, and requires rejection when the encoded aggregate is absent
from that lineage's validated bundle. Current context-free constructors can
prove only the `PartitionKey` envelope. Passing such a grant into policy or the
commit coordinator would treat unvalidated component bytes as authorization
identity.

The POC has one active lineage. A compatible successor cannot remove an
aggregate or change its partition `KeySchema`; those changes are incompatible
under the accepted catalog policy. Consequently, schema validation against the
active validated bundle remains valid if a compatible activation races after
the preparatory read. No catalog version or bundle hash needs to enter
capability replay identity.

Before the first contract is deployed there is no active bundle from which to
select a partition schema. Bootstrap occurs in that pre-active lifecycle. The
database must either reject explicit partition scopes then, accept an
envelope-only authorization identity, or add a schema/version carrier to the
bootstrap request. Only the first option preserves the accepted fail-closed
boundary without changing the public or durable format.

## Proposed Decision

### Validation stages and owners

Public key validation has two distinct stages. Neither stage may be described
as the other:

1. **Context-free envelope validation** is owned by `riffdb-types` and invoked
   by `riffdb-proto` during WP-127 structural validation. It checks the complete
   byte bound, minimum envelope length, purpose byte, supported key-codec
   version, and nonzero encoded owner ID. When the same public message carries
   a separate owner ID and that comparison needs no catalog, it also checks
   exact owner agreement. Successful envelope validation produces only the
   existing purpose-specific opaque key newtype. It does not prove component
   count, component types, variable-length boundaries, UTF-8, declared bounds,
   enum membership, or full schema-directed consumption.
2. **Schema-directed semantic validation** is owned by the API-neutral shared
   service using a `ValidatedContractBundle` selected through its catalog read
   port and the exact `riffdb-contract-ir::KeySchema`. It checks the expected
   lineage and owner, resolves the owner in the selected bundle, invokes the
   purpose-specific schema decoder, and requires complete consumption. Only
   this stage may claim that a public key is a complete canonical key for the
   selected contract schema.

For public requests that already select a contract, such as `GetEntity`, the
shared service performs stage two after resolving that selection and before
policy evaluation or an authoritative read. For service-produced entity and
index results, the service validates keys with the selected schema before
releasing the result to an adapter. A public client receiving an opaque result
key can context-free validate its envelope but does not claim schema validation
without the corresponding checked schema.

WP-127 owns bounded wire preflight, Prost decoding, stage-one calls, structural
cross-field checks, descriptors, hashes, and fixtures. It neither imports
contract IR nor resolves a catalog. ADR-0028's phrases "decode fully through
their semantic key codec," "fully decode through their canonical key codecs,"
and equivalent public-key wording mean completion of both stages at the full
request or response handling boundary, not completion inside `riffdb-proto`.

WP-130 converts wire values into the existing checked envelope/key and
API-neutral DTO types, then invokes the shared service. Its gRPC adapters do not
query the catalog, choose a `KeySchema`, implement component decoding, or
silently strengthen an envelope into a semantic proof. Its startup composition
may continue to connect already accepted catalog and readiness ports, but
transport conversion gains no catalog access.

### Explicit capability partition scopes

`riffdb-service` owns one bounded schema-validation operation for a capability
grant's explicit partition scope. The operation accepts the grant plus a
catalog-validated active bundle and, for every canonical scoped entry:

1. requires the entry lineage to equal the active bundle lineage;
2. reads the nonzero `AggregateTypeId` from the already checked partition-key
   envelope;
3. resolves that aggregate in the active bundle;
4. obtains that aggregate's exact partition `KeySchema`; and
5. calls `decode_partition`, requiring successful full consumption.

Unknown lineages, absent aggregates, wrong purposes or owners, malformed
components, out-of-bound component values, unknown enum variants, trailing
bytes, catalog absence, and catalog failure all fail closed. Public validation
uses the existing generic safe validation or availability classification and
does not expose the lineage, aggregate, key bytes, schema, or decoder reason.
Raw keys remain redacted in diagnostics and telemetry.

Normal capability creation enters ADR-0007's intrinsic pre-start audit scope as
soon as its closed operation and capability target are classified, then performs
this bounded active-catalog preparation and schema validation before the first
capability-policy evaluation, `started` record, token issuance, control-plane
permit, or coordinator submission. Invalid input or a proven preparatory failure
appends exactly one standalone `failed` record; cancellation or deadline appends
exactly one standalone `cancelled` record. The existing ADR-0007 audit-outage
matrix remains authoritative. A private service preparation/proof may make the
success ordering unrepresentable, but it is process-local scaffolding and is not
a public, policy, storage, IR, or durable type. The validated active version and
bundle hash do not become fields of `NormalizedCapabilityCreateRecord` and do
not change capability-create replay equality.

`PartitionScopeV1::All` carries no public key and requires no key-schema decode.
An explicit scope requires an active validated bundle. Therefore a bootstrap
request made before an active contract exists rejects an explicit partition
scope with the same generic fail-closed validation result. Pre-active bootstrap
continues to support all-partitions scope, explicit tenant scope, and explicit
recognized permission atoms, including `ADMINISTER_CAPABILITIES`; it receives
no marker-backed or bootstrap-only key-validation exemption. Exact bootstrap
replay remains possible because a successfully committed pre-active bootstrap
could not have contained an explicit partition scope under this rule.

The service-owned check is also the semantic check reused when the production
startup composition verifies active, unexpired capability records before
readiness. WP-130 may wire the already accepted structural/catalog evidence to
that check, but does not reimplement it in gRPC or server code. An active
capability whose explicit scope cannot be validated against the active bundle
prevents readiness. The current POC's compatible-successor rules make the
active schema sufficient for lineage-scoped capability partitions; support for
multiple active lineages or partition-key schema migration requires a new ADR.

### No proof laundering

No crate may name an envelope-only key `validated`, `canonical`, or `decoded`
in a way that implies schema-directed component validation. Existing opaque
purpose-specific key newtypes remain valid stage-one values; this ADR does not
add a second byte representation. Policy compares exact `ScopedPartitionV1`
values only after the service has established the required stage-two proof for
untrusted create input or startup state. Runtime/compiler-derived partition
keys retain their existing schema-directed construction guarantees.

## Options Considered

1. **Two-stage validation in protocol and shared service:** Proposed. It matches
   the information available at each boundary and keeps transports semantic-
   free while preserving fail-closed authorization.
2. **Make `riffdb-proto` depend on contract IR or catalog:** Rejected. A generic
   decoder still lacks the selected bundle, and the dependency would violate
   the accepted protocol and service ownership boundaries.
3. **Resolve schemas in WP-130 adapters:** Rejected. gRPC would acquire semantic
   behavior not shared automatically by MCP, CLI, or an in-process service
   caller.
4. **Treat envelope validation as complete validation:** Rejected. Component
   bytes have no type tags, so malformed or noncanonical bytes can satisfy the
   envelope.
5. **Permit an envelope-only explicit bootstrap partition:** Rejected. It would
   create durable authorization state that no validated schema proved and would
   contradict ADR-0009's no marker-backed exemption.
6. **Add contract version, bundle bytes, or schema hash to capability create:**
   Rejected for the POC. It changes the accepted public request, replay identity,
   and potentially durable compatibility boundary solely to enable a pre-active
   explicit partition scope.

## Consequences

- WP-127 can implement honest, context-free key validators and fixtures without
  a prohibited dependency.
- Every transport obtains identical schema-directed request semantics through
  `riffdb-service`; MCP and gRPC gain no privileged path.
- WP-120 must close the explicit-capability-scope gap before its capability
  service semantics merge. WP-127 or WP-130 cannot repair that gap later.
- Bootstrap before deployment cannot create an explicitly partition-scoped
  capability. The bootstrap capability can use all-partitions scope and later
  delegate an explicit scope after contract activation.
- One additional bounded catalog preparation is required for normal capability
  creation with an explicit partition scope. Compatible activation does not
  invalidate the result.
- Multi-lineage catalogs, key-schema migration, and pre-active schema-bearing
  bootstrap grants remain deferred beyond the POC.

## Compatibility

This decision changes no Protobuf package, message, field number, wire bytes,
key bytes, hash preimage, contract grammar, IR encoding, storage record, durable
envelope, capability replay identity, or MCP schema. No migration is required.

It clarifies ADR-0028's validation ownership rather than weakening the required
end-to-end guarantee: a handled request or produced result still receives full
schema-directed validation wherever a complete canonical key is required.
Stage one alone simply stops claiming that guarantee.

The pre-active rejection of explicit bootstrap partition scopes is a fail-
closed semantic restriction within the already accepted request shape. Adding
that capability later requires an additive, separately reviewed schema source
and replay-identity decision.

## Security

Partition keys are authorization evidence, not arbitrary opaque caller labels.
Schema-directed validation must finish before policy can compare or delegate an
untrusted explicit scope. Failure remains generic and redacted so callers
cannot probe installed aggregate IDs, component schemas, enum registries, or
another capability's scope.

Catalog resolution is preparatory and produces no caller-visible data. The
service does not hold a storage transaction while validating keys. Cancellation,
deadline, catalog unavailability, missing active state, and malformed input all
release nondurable capacity and fail closed before token generation or
authoritative mutation, while preserving ADR-0007's required normal-create
standalone audit phase. Invalid or mismatched principal-less bootstrap attempts
remain unaudited under ADR-0007's bootstrap exception.

## Testing

WP-127 freezes stage-one fixtures for every public entity, index-entry, and
partition key position: minimum and maximum lengths, one-over maximum, wrong
purpose, unsupported codec version, zero owner, separately carried owner
mismatch, valid envelope with malformed component bytes, and trailing bytes.
The last two must pass envelope construction but must not be labeled
schema-valid.

WP-120 adds service tests proving that explicit capability scopes accept exact
valid keys and reject wrong lineage, absent aggregate, wrong component type or
length, invalid UTF-8, out-of-bound text/bytes, unknown enum variant, and
trailing bytes. Normal-create ordering tests prove rejection occurs before
policy, token issuer, control-plane permit, and coordinator calls, with exactly
one standalone `failed` or `cancelled` audit phase and no `started` record.
Tests also prove `All` needs no key decoder, pre-active explicit bootstrap
rejects without policy, audit, or durable effect, and concurrent compatible
activation cannot change the accepted partition schema.

WP-130 architecture tests prove gRPC adapters have no catalog or contract-IR
dependency and that all public key inputs reach the shared service. Startup
tests inject a malformed explicit scope into active structural evidence and
prove readiness remains false. WP-140 proves MCP reaches the same service check
and cannot bypass it.

## Requirements and Work Packages

- **Requirements:** `API-001`, `SEC-001`, `STO-002`, `VAL-003`
- **Defines or blocks:** capability-scope completion in `WP-120`, public key
  validation in `WP-127`, and adapter/readiness composition in `WP-130`
- **Consumed later by:** `WP-140`, `WP-150`, `WP-190`, and `WP-200`
- **Final evidence:** `WP-200`

## Decision Deadline

Exact acceptance is required before WP-120 merges capability-create semantics
that can admit explicit partition scopes and before WP-127 freezes public key
validation fixtures. WP-130 must not compensate for a missing decision by
adding catalog-aware transport conversion.
