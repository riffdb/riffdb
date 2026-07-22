# ADR-0031: Schema-Directed Submitted Command Values

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0005, ADR-0006, ADR-0007, ADR-0011, ADR-0013,
  ADR-0028, and ADR-0029
- **Amends:** ADR-0007 command-request DTO and transport-conversion wording
- **Decision deadline:** Before WP-130 implements Execute conversion

The human maintainer approved this decision and its authoritative-file
amendments on 2026-07-21. This record repairs an internal Rust handoff. It does
not change a public Protobuf field, canonical value encoding, durable record,
hash preimage, service method, RPC, or POC feature.

## Context

The public `riffdb.v1.Decimal` intentionally carries a minimal signed
coefficient and scale, but not precision. ADR-0006 and ADR-0011 require the
compiled decimal precision and scale before constructing a canonical
`Decimal`. Public record fields may use a stable field ID, a source name, or
both; names require the selected compiled record schema and an ID/name pair must
agree. Enum display names likewise require the selected enum schema.

`riffdb-proto` can therefore prove structural bounds and encoding but cannot
produce a schema-canonical `CanonicalRecord`. It has no contract IR or catalog
dependency. ADR-0007 and ADR-0029 also forbid a gRPC adapter from selecting a
catalog schema or interpreting command semantics.

The first WP-120 interface nevertheless made `ExecuteCommandRequest` require a
`CanonicalRecord` before the service selected the exact active or historical
command plan. A gRPC adapter cannot construct that request for all valid public
inputs without guessing decimal precision, discarding names, or gaining
forbidden catalog access. Adding precision to the public decimal would alter an
accepted wire boundary and let callers supply redundant type identity.

## Decision

### Service-owned submitted-value family

`riffdb-service` owns one API-neutral, checked, pre-schema submitted-value
family. Its public names are `SubmittedValue`, `SubmittedDecimal`,
`SubmittedMoney`, `SubmittedEnum`, `SubmittedList`, `SubmittedRecord`,
`SubmittedField`, and `SubmittedFieldIdentity`, or mechanically equivalent
names with the same ownership and guarantees.

The family preserves exactly the information needed for later materialization:

- scalar values that need no missing type context;
- decimal coefficient and scale without an invented precision;
- money currency plus its submitted decimal;
- enum type ID, variant ID, and optional display name;
- bounded recursive lists and records; and
- for each record field, an optional nonzero stable field ID, an optional
  bounded source name, and the requirement that at least one is present.

Constructors enforce the existing document, string, bytes, name, collection,
and recursion ceilings with checked arithmetic. They preserve structurally
meaningful identity but do not resolve names, fill omitted optional fields,
choose a decimal precision, or claim compiled-schema validity. Formatting is
redacted. These values are not serializable, persistable, canonical hash
inputs, policy facts, runtime values, or storage values.

`ExecuteCommandRequest` contains a `SubmittedRecord`. A checked conversion from
an already canonical record may be supplied for trusted API-neutral callers,
but it still produces submitted values and does not bypass service
materialization. No additional application-service operation or trait method is
added.

### Transport conversion

After bounded Protobuf decoding and ADR-0028 structural validation, WP-130 maps
`riffdb.v1.Value` mechanically into the submitted-value family. This mapping
does not load a catalog, import contract IR, choose a command plan, resolve a
field or enum name, construct a decimal with guessed precision, hash input, or
make a policy decision.

Every later transport, including MCP and CLI paths, constructs the same
submitted DTO or calls the same shared application service. No transport may
introduce an alternate schema-aware conversion path.

### Schema-directed materialization

After selecting the exact checked command plan and schema, `riffdb-service`
materializes the submitted record into the sole `CanonicalRecord` used by the
rest of command preparation. It:

1. resolves each stable ID or source name against the selected record schema;
2. requires a supplied ID and name to identify the same field;
3. rejects duplicate fields after resolution and emits increasing stable IDs;
4. constructs decimal values with the declared precision and exact submitted
   scale, rejecting coefficient overflow or scale mismatch;
5. constructs money with the declared currency and fixed money decimal type;
6. requires enum type/variant membership and, when supplied, exact display-name
   agreement;
7. recursively materializes lists and referenced records under their exact
   compiled schemas and bounds; and
8. fills only schema-declared omitted optional fields with canonical null while
   retaining the accepted historical optional-field compatibility rules.

Materialization returns bounded public validation for caller-correctable
failure and an internal integrity failure only for an impossible checked
plan/schema state. The produced canonical record is the first representation
eligible for input hashing, idempotency identity, input-computable facts,
authorization, command admission, runtime, persistence, or provenance.

For an absent idempotency identity, the service materializes against the active
plan before extracting the contract-declared idempotency key. For retained
pending or terminal identity, it rematerializes against each exact selected
historical plan before comparing canonical input. It never treats active-plan
materialization as proof for a different historical plan.

## Options Considered

1. **Service-owned submitted values followed by plan-directed materialization:**
   Accepted. It keeps every transport structural and gives semantic conversion
   one owner with the required schema.
2. **Add decimal precision and more schema identity to public values:** Rejected.
   It changes the frozen wire contract and creates redundant caller-selected
   type claims.
3. **Let gRPC query the catalog:** Rejected. It violates the shared-service
   boundary and would not automatically protect MCP, CLI, or in-process calls.
4. **Guess maximum precision or ignore names:** Rejected. Exact decimal specs
   participate in type validity, canonical hashes, and deterministic arithmetic;
   discarding names changes accepted public semantics.
5. **Make `riffdb-proto` depend on contract IR/catalog:** Rejected. A generic
   protocol decoder still lacks the selected active or historical bundle and
   would invert the accepted dependency direction.

## Consequences

- WP-120 receives a focused interface correction before WP-130.
- WP-125's service adapter constructs submitted input rather than relying on a
  transport-only shortcut.
- WP-130 conversion is total without a catalog dependency.
- Canonical hashing, idempotency, policy facts, and runtime remain unchanged
  because they continue to consume only the materialized `CanonicalRecord`.
- The public SDK may expose ergonomic typed builders while its generic dynamic
  path preserves the same submitted semantics.

## Compatibility

This changes internal Rust service DTOs only. It changes no Protobuf package,
message, field, tag, presence rule, value encoding, canonical record encoding,
contract grammar, IR encoding, plan hash, canonical input hash, idempotency
identity, storage record, durable envelope, command ordering, or public outcome.
No migration or protocol version change is required.

## Security

Untrusted values remain bounded before allocation-heavy or schema-aware work.
Semantic conversion occurs before hashing, policy evaluation, admission, or
persistence, so a name, decimal, enum, or nested record cannot acquire trusted
meaning through a transport-specific shortcut. Failures do not echo submitted
business values or compiled schema detail. Redacted formatting applies before
tracing, diagnostics, and public errors.

## Testing

WP-120 freezes submitted-value construction limits and schema materialization
for ID-only, name-only, agreeing and disagreeing ID/name fields; duplicate
resolved fields; decimal scale/precision/coefficient boundaries; money
currency; enum membership/name agreement; nested list/record bounds; optional
null filling; historical optional additions; and canonical input-hash parity
between ID and name representations.

WP-130 proves every public `Value` branch maps mechanically to submitted values,
that the gRPC crate has no catalog or contract-IR dependency, and that valid
`decimal<28,2>` budget input reaches the same canonical service value as the
typed SDK builder. WP-140 proves MCP uses the same service materialization.

## Requirements and Work Packages

- **Requirements:** `API-001`, `ID-001`, `VAL-001`, `VAL-003`, `POC-004`
- **Corrects:** `WP-120`
- **Blocks:** `WP-130`
- **Consumed by:** `WP-125`, `WP-127`, `WP-130`, `WP-135`, `WP-140`, and
  `WP-200`
- **Final evidence:** `WP-200`
