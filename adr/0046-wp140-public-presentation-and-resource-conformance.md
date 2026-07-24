# ADR-0046: WP-140 Public Presentation and Resource Conformance

- **Status:** Accepted
- **Direction approved:** 2026-07-23
- **Exact text accepted:** 2026-07-23
- **Accepted:** 2026-07-23
- **Acceptance reference:** Human maintainer confirmation in the current Codex
  session on 2026-07-23
- **Requires:** ADR-0006, ADR-0007, ADR-0008, ADR-0011, ADR-0027,
  ADR-0028, ADR-0031, ADR-0040, and ADR-0044
- **Amends:** ADR-0008's post-authentication rate-limit scope and tagged-value
  presentation boundary; ADR-0040's WP-137 public bridge, ownership, and exact
  public message inventory; SPEC Sections 6.2, 11.2, and 12.2 through 12.12;
  WP-137 and WP-140; and their generated public/MCP compatibility fixtures
- **Decision deadline:** Before WP-140 production adapters or corrected
  resource-content fixtures merge

The human maintainer accepted the correction set after implementation exposed
four missing facts at the public/MCP boundary: public decimals could not carry
their checked precision, name-only MCP enums could not reach schema-directed
service resolution, dynamic outcome values lost schema names, and contract
resources could not present the bundle's compatibility metadata. This record
also corrects resource goldens that omitted required links, schemas, examples,
or projection lag, and aligns rate limiting with the HTTP-only `MCP-042`
requirement.

## Context

ADR-0008 correctly requires MCP decimal and money values to carry
precision/scale/coefficient and requires dynamic tool results to conform to the
compiler-owned natural JSON outcome schema. The public Protobuf `Decimal`
currently carries only coefficient and scale. The service therefore cannot
distinguish a caller's asserted precision from precision supplied by the
selected schema, and a public response cannot reconstruct the accepted MCP
tagged decimal without inventing a value.

The public `EnumValue` already has stable IDs and a name, but the structural
gRPC conversion rejects zero IDs before the selected operation schema is known.
Compiler-generated MCP input schemas expose enum source names, not internal
numeric IDs, so stdio needs one explicit name-only submitted form.

`DeclaredOutcomeView` validates a canonical outcome against the exact historical
schema, then discards the field and enum names needed to render that outcome
against the compiler-owned natural JSON Schema. An adapter must not infer names
from array position or rediscover a now-active bundle.

The immutable bundle already contains a parent reference and a bounded
compatibility report of at most 4,096 path findings. `ContractDescriptor`
currently exposes neither. Copying every path into every descriptor can exceed
the public response budget, while omitting all compatibility data fails the
contract-version resource defined by SPEC Section 12.6.

Finally, the accepted WP-140 resource checkpoint used placeholder content that
did not meet its own normative descriptions: active-contract content lacked an
exact-version link, command-plan content lacked generated schemas,
command-documentation content invented prose and contained no generated
example, and projection status lacked lag. Those are checkpoint defects, not
reasons to weaken the specification.

## Decision

### Public decimal precision

`proto/riffdb/v1/value.proto` appends exactly:

```protobuf
message Decimal {
  bytes coefficient_twos_complement = 1;
  uint32 scale = 2;
  optional uint32 precision = 3;
}
```

No existing field is renumbered or retyped. Precision is in `1..=38`. When
present, scale must not exceed precision and the coefficient magnitude must fit
precision. When absent, structural conversion preserves absence and the
selected operation schema supplies precision. In either case, schema-directed
materialization requires the submitted scale to equal the selected fixed scale;
when precision was supplied, it must equal the selected fixed precision. The
optional field is an assertion, not a caller-selected type.

`SubmittedDecimal` retains `Option<DecimalSpec>`-equivalent precision evidence
without creating a canonical decimal before schema selection. Money applies the
same rules to its amount and continues to require exact schema-selected
currency. Name-only inputs and legacy absent-precision inputs therefore remain
pre-schema submitted values.

A new server always emits `precision` for every public decimal or money value
because canonical `riffdb-types::Decimal` already owns the exact
`DecimalSpec`. General public SDK decoding remains additive and may accept an
absent field from an older server. The MCP stdio adapter fails closed if a
decimal-bearing response from an older server lacks precision; it never guesses.

This field changes no canonical value encoding, canonical input hash, contract
schema, IR, bundle hash, plan hash, durable message, or storage key.

### Enum submission and schema resolution

The existing `EnumValue` fields admit exactly two submitted identity forms:

1. `type_id` and `variant_id` are both nonzero, with `name` either empty or a
   checked source name. A supplied name is redundant evidence and must match the
   exact selected schema.
2. `type_id` and `variant_id` are both zero and `name` is a nonempty checked
   source name. The service resolves that name only within the enum type required
   at the exact value position of the selected operation schema.

One zero ID with one nonzero ID, or two zero IDs with an empty name, rejects
structurally. Name-only resolution is case-sensitive, performs no
normalization, and rejects absence or ambiguity. It does not permit an adapter
to select a different enum type.

The common MCP input converter emits the second form for natural compiler-schema
enum strings. Both transports then use the same service materializer. No public
field, enum registry, compiler artifact, or durable value changes.

### Schema-bound declared outcome presentation

`riffdb-service` remains the sole owner of the public-safe
`DeclaredOutcomeView`. Construction against an exact checked historical
`SchemaIr` and `OutcomeSchema` additionally retains one private, bounded,
schema-bound presentation plan containing only:

- record field stable ID and exact source name;
- enum type/variant stable IDs and exact variant source name; and
- the recursive list/record shape needed to join those names to the already
  validated canonical value.

The presentation plan is derived only while validating the exact outcome. It
contains no authority, mutable catalog reference, storage handle, raw source,
or independent semantic value. Its construction and traversal obey the
existing nesting, collection, schema-artifact, and public-response bounds.

The gRPC conversion for `ExecuteCommandResponse` and a found
`GetOutcomeResponse` traverses the canonical value with that plan. Every
schema-bound `ValueField` carries both its nonzero stable ID and exact `name`;
every schema-bound `EnumValue` carries both nonzero IDs and its exact variant
`name`. The hosted backend consumes the same service view. The common MCP
renderer converts the schema-bound presentation directly to the natural object
and enum-string form required by the exact compiler outcome union.

Generic canonical-value conversion for entity, commit, projection, and other
fixed tagged-value results remains ID-based. Adapters must not infer a field
name from record position, an enum name from an ordinal, or metadata from the
active catalog. Existing Protobuf fields suffice; this decision adds no outcome
wire field and changes no durable outcome.

### Bounded public compatibility summary

`proto/riffdb/v1/contract.proto` appends these public shapes:

```protobuf
enum ContractCompatibilityClass {
  CONTRACT_COMPATIBILITY_CLASS_UNSPECIFIED = 0;
  CONTRACT_COMPATIBILITY_CLASS_COMPATIBLE = 1;
  CONTRACT_COMPATIBILITY_CLASS_REQUIRES_EXPLICIT_VERSION = 2;
  CONTRACT_COMPATIBILITY_CLASS_INCOMPATIBLE = 3;
}

message ContractCompatibilityCodeCount {
  string code = 1;
  uint32 count = 2;
}

message ContractCompatibilitySummary {
  optional uint64 parent_contract_version = 1;
  optional bytes parent_bundle_hash = 2;
  ContractCompatibilityClass overall = 3;
  repeated ContractCompatibilityCodeCount code_counts = 4;
}

message ContractDescriptor {
  // Existing fields 1 through 5 remain unchanged.
  ContractCompatibilitySummary compatibility = 6;
}
```

The new server always supplies `compatibility`. A genesis bundle has both parent
fields absent, class `COMPATIBLE`, and no code counts. A successor has both
parent fields present and equal to its checked `ParentBundleRef`. Counts contain
only nonzero counts, in strictly increasing exact `CompatibilityCode::as_str()`
order, with at most the closed 20-code registry, no duplicates, each count in
`1..=4096`, and a total in `1..=4096`. `overall` equals the immutable report's
most restrictive class. Parent fields are either both present or both absent.

The service creates this summary from the already validated immutable
`ContractBundle`; adapters do not compare bundles or classify changes. Full
affected paths remain in the bundle/compiler interfaces and are deliberately
not copied into this frequently returned public descriptor. This is a bounded
summary, not a replacement for the authoritative compatibility report.

An older server may omit field 6 and a general new SDK preserves additive
compatibility. A WP-140 contract-version resource requires the summary and fails
closed rather than fabricating compatibility metadata.

### Corrected MCP resource content

The existing URI registry, MIME types, list-surface membership, subscription
set, and service lookup algorithms remain unchanged. WP-140 replaces only the
incorrect content goldens and their common renderers:

- `riffdb://contract/active` contains the descriptor plus a canonical
  `links.contract_version` URI for that exact lineage and version.
- `riffdb://contract/<lineage>/<version>` contains the descriptor and the
  bounded compatibility summary. Genesis parent is JSON `null`; a successor
  parent contains canonical decimal version text and lowercase bundle-hash hex.
  Code counts preserve their public canonical order.
- `riffdb://command/<lineage>/<command-id>/plan` contains the exact checked
  command identity, plan hash, public explanation, and the exact compiler-owned
  input and outcome JSON Schema objects returned by `ExplainCommand`. It does
  not substitute schema debug text or a second schema source.
- `riffdb://projection/<lineage>/<projection-id>/status` contains `lag` in
  addition to the complete public status. When a published frontier exists,
  lag is the canonical unsigned-decimal-string distance from that frontier to
  the authoritative head, treating `before_first` as ordinal zero. The service
  invariant already requires the frontier not to exceed the head. When no
  published frontier exists, lag is JSON `null`.

Active and version content use one common descriptor renderer. Plan content
uses the exact fenced `ExplainCommand` result already required by ADR-0008.
Projection lag is presentation-only and is never persisted or treated as a
second frontier.

### Factual generated command documentation

Command documentation is generated in common `riffdb-api-mcp` code from the
exact descriptor, fenced `ExplainCommand` result, and its two compiler-owned
schemas. It contains only:

- the escaped exact command source name as the heading;
- bounded factual identity and execution-plan fields already returned by the
  service;
- the compiler-rendered explanation as escaped/preformatted content;
- for an idempotent-mutation execution class, the fixed safety notice
  `Cancellation after command submission may not prevent commit. Resolve an
  uncertain result with the same idempotency key or the returned outcome URI.`;
- one deterministic minimal input example; and
- one deterministic minimal example for each declared-outcome `oneOf` branch,
  in compiler schema order.

There is no hand-authored business prose, guessed intent, model-authored text,
or raw unescaped Markdown. The example generator supports only the closed JSON
Schema subset emitted by the accepted compiler. It chooses explicit `const`
before the first `enum` member; includes every required object property; uses
canonical property order; selects deterministic in-range scalar values from
the compiler's type annotations; satisfies tuple/minimum array requirements;
and traverses bounded recursion and collection counts. Unsupported or
unsatisfiable schema shape fails the resource read.

Every generated example is run back through the same common Draft 2020-12
validator before Markdown release. Generation or validation failure is a
bounded resource failure, never a partially rendered document. Schema JSON and
examples are fenced as data and all dynamic Markdown text uses the existing
injection-safe builder. The complete resource remains within the 4 MiB MCP
outbound bound.

### Post-authentication limiter scope

ADR-0008's statement that both transports perform the second adapter-owned
token-bucket check is superseded. `MCP-042` applies the exact second bucket only
to hosted Streamable HTTP, where the adapter possesses the authenticated
principal, policy-resolved tenant, and decoded operation or compiler-owned tool
name before protected service work. The separate pre-authentication bucket owns
the trusted socket-peer address component of `MCP-042`.

Production stdio is an ordinary public gRPC client and has no local
`AuthenticatedPrincipal`, policy decision, or tenant authority. It therefore
does not recreate this bucket from untrusted MCP or credential presentation.
Each stdio call still passes through the normal authenticated gRPC admission,
service authorization, service limits, audit, and server-wide resource bounds.
Transport equivalence concerns authorized tools, resources, and semantic
results, not duplicate ingress throttling at a different trust boundary.

The post-authentication hosted key remains `(McpHttp, principal, policy tenant,
service operation or exact compiler-owned tool)`.
The accepted burst/refill, registry capacity, expiry, monotonic-clock, and
fail-closed rules are unchanged. The pre-authentication peer-IP bucket remains
HTTP-only and unchanged.

### Corrective package ownership

WP-137 is narrowly reopened only for the additive public bridge and
schema-bound service presentation required above. ADR-0046 is added to the
required ADRs for WP-137 and WP-140. WP-137 additionally permits:

```text
crates/riffdb-service/src/contract_operations.rs
crates/riffdb-service/src/submitted.rs
```

Its existing whole-crate public proto, gRPC adapter, client, generated-fixture,
fuzz, and script paths already cover the remaining corrections. WP-140 owns the
common MCP resource/documentation renderers, corrected MCP goldens, hosted-only
limiter use, and transport conformance.

There is no new work-package dependency or gate change. WP-140 remains blocked
on corrected WP-137 acceptance evidence. No new dependency, Cargo feature,
service operation, RPC, transport route, resource URI, durable format, storage
API, compiler format, or contract-language feature is authorized.

## Options Considered

1. **Infer decimal precision from coefficient or scale:** rejected; multiple
   declared precisions share the same value representation.
2. **Put MCP-only IDs into generated enum schemas:** rejected; it forks the
   compiler schema and makes stdio differ from other natural JSON consumers.
3. **Have adapters rediscover schemas for outcome names:** rejected; it is
   race-prone, duplicates semantic joins, and can select the wrong version.
4. **Expose all compatibility paths in every descriptor:** rejected; the
   accepted report permits 4,096 paths of up to 1,024 bytes each.
5. **Keep placeholder resource goldens:** rejected; accepted fixtures cannot
   weaken the specification they are intended to freeze.
6. **Rate-limit stdio using credential or model-provided identity:** rejected;
   those bytes are not authenticated principal or policy facts in that process.

## Consequences

- Public Protobuf descriptors, generated Rust, schema inventory/hashes, wire
  vectors, response-charge fixtures, and client conversions change additively.
- A new server can faithfully render every accepted MCP decimal, money,
  natural enum input, dynamic declared outcome, and contract-version resource.
- Dynamic outcome presentation remains joined to the exact historical plan
  without changing durable values.
- Full compatibility findings remain available at compiler/bundle boundaries;
  public descriptors intentionally expose only a complete bounded count summary.
- Correct command documentation is factual and mechanically reproducible, but
  unsupported future compiler-schema constructs require a reviewed generator
  extension.

## Compatibility

The public changes append one optional scalar, one message field, and new
message/enum symbols. Existing field numbers, enum values, services, methods,
streaming shapes, schema IDs, URI bytes, and durable messages remain unchanged.
Old clients ignore the additions. General new clients accept their absence from
old servers, while WP-140 adapters fail closed only when the missing fact is
required to satisfy an MCP schema or resource contract.

Changing the precision tag, compatibility tags/classes/count semantics,
name-only enum interpretation, schema-bound presentation rules, resource JSON
shape, documentation algorithm, or corrected goldens requires public
compatibility review.

## Security

Schema-bound names come only from a checked historical bundle and are escaped
before Markdown. Examples are generated from compiler artifacts and validated
before release. Compatibility summaries contain no source text or affected
business path. Adapters receive no catalog/storage authority. The hosted limiter
uses only authenticated and trusted facts; stdio does not mislabel untrusted
presentation as authentication evidence.

## Testing

WP-137 adds:

- old/new absent/present decimal precision compatibility and exact
  schema-assertion tests for decimal and money;
- all accepted/rejected enum identity forms and exact schema-name resolution;
- schema-bound nested record/list/enum outcome presentation, including
  historical replay after active-version change;
- compatibility-summary genesis/successor/count/order/bound conversions;
- descriptor and generated-artifact compatibility checks;
- public decoder fuzzing for new presence and count boundaries; and
- clean `scripts/generate-proto --check` evidence.

WP-140 adds:

- byte-identical hosted/stdio natural dynamic outcomes with field and enum names;
- decimal/money precision and enum-name input conformance;
- corrected active/version/plan/docs/projection resource goldens;
- generated-example validation, determinism, bounds, and Markdown-injection
  canaries;
- hosted post-authentication rate-key separation and failure schedules; and
- evidence that stdio contains no principal/tenant derivation or local
  post-authentication bucket.

## Requirements and Work Packages

This record clarifies `STO-002`, `VAL-001`, `API-001`, `MCP-010`, `MCP-020`,
`MCP-021`, `MCP-030`, `MCP-033`, `MCP-040`, `MCP-042`, `MCP-043`, `MCP-045`,
and `MCP-049`. It is implemented by the corrective WP-137 slice and WP-140.

## Decision Deadline

Accepted before the first production WP-140 adapter and corrected resource
checkpoint merge. Any incompatible alternative requires a new ADR and human
review.
