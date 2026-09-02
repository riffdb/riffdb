# ADR-0179: Generated Public-Surface Adapters and the Operation Registry

- **Status:** Accepted
- **Obligations:**
  - `OBL-0179-1` WP-751 must prove that every public operation has exactly
    one registry declaration and `riffdb-proto` contains no hand-written
    exchange validation outside its generated directory. Proof:
    `every_public_operation_has_exactly_one_registry_declaration`.
  - `OBL-0179-2` WP-751 must prove that generated validation, conversion,
    MCP schema, and CLI envelope output reproduces the frozen pre-registry
    fixtures byte for byte. Proof:
    `generated_adapters_reproduce_frozen_public_fixtures_byte_for_byte`.
  - `OBL-0179-3` WP-753 must prove that every binding returns the same
    closed public error class for every entry of the shared drift corpus;
    planned proof
    `every_binding_returns_the_same_error_class_for_the_shared_corpus`.
  - `OBL-0179-4` WP-753 must prove that the four language generators render
    from one canonical model and reproduce their golden outputs; planned
    proof `template_generators_reproduce_golden_outputs_across_languages`.
  - `OBL-0179-5` WP-753 must prove that a change that adds one public
    operation hand-touches only the Protobuf source, the registry, the
    service implementation, and the handbook; planned proof
    `check-three-places`.
  - `OBL-0179-6` WP-752 must prove that no file on the adapter path exceeds
    the decomposition budget. Proof:
    `adapter_path_files_stay_under_the_decomposition_budget`.
  - `OBL-0179-7` WP-752 must prove that the other five adapter crates contain
    no hand-written conversion or dispatch outside their generated
    directories. Proof:
    `five_adapter_crates_contain_no_hand_written_conversion_or_dispatch`.
- **Direction approved:** 2026-09-01
- **Exact text accepted:** Yes, 2026-09-01
- **Accepted:** 2026-09-01
- **Acceptance reference:** Maintainer acceptance of the exact text in the
  current Claude Code session on 2026-09-01, all seven consolidation records together
- **Decision deadline:** Before WP-751 adds the operation registry or moves
  any public-message validation bound out of hand-written source
- **Requires:** ADR-0006, ADR-0007, ADR-0008, ADR-0020, ADR-0027, ADR-0028,
  ADR-0037, ADR-0040, ADR-0041, ADR-0064, ADR-0106, ADR-0118, ADR-0124, and
  ADR-0148
- **Amends:** ADR-0007's DTO and interface ownership, ADR-0040's public Rust
  client surface, and ADR-0041's public-only command flow only where they
  describe hand-written conversion, validation, or dispatch as the owning
  implementation; SPEC 5.3's final rule on checked-in generated code is
  extended from Protobuf, JSON Schema, and SDK output to every public-surface
  adapter
- **Defines or blocks:** WP-751 through WP-753

The maintainer accepted the exact text of this record on 2026-09-01. Its packages
may begin. Each deferred obligation above is tracked in
`adr/obligations-outstanding.yaml` until its planned proof exists, at which
point the owning package discharges it by declaring the proof.

## Amendment 1 — adapter architecture-proof ownership split (Accepted 2026-09-01)

The original `OBL-0179-1` joined two package-sized architecture proofs. This
amendment splits their ownership without weakening the generated-adapter
boundary:

- **WP-751 owns `OBL-0179-1`.** Every public operation has exactly one registry
  declaration, and `riffdb-proto` contains no hand-written exchange validation
  outside its generated directory. Its proof remains
  `every_public_operation_has_exactly_one_registry_declaration`.
- **WP-752 owns `OBL-0179-7`.** The other five adapter crates contain no
  hand-written conversion or dispatch outside their generated directories. Its
  proof is
  `five_adapter_crates_contain_no_hand_written_conversion_or_dispatch`.

Moving the whole original obligation to WP-752 would leave WP-751 with no
architecture obligation over its main deliverable. Combining WP-751 and WP-752
would remove the fixture-freeze step that makes replacement of the remaining
five adapters safe to land. The package boundary and landing order therefore
remain unchanged; only the proof obligation is split at that boundary.

Status: Accepted by the maintainer on 2026-09-01.

## Context

Every public operation is currently expressed by hand in six layers between
the Protobuf source and an application. The 57 kernel RPCs in
`proto/riffdb/v1/services.proto` and 10 application RPCs in
`proto/riffdb/app/v1` map to 57 `ServiceOperationV1` variants in
`crates/riffdb-types/src/service.rs`. Each is then re-expressed as a bounded
validation exchange in `crates/riffdb-proto/src/public_message.rs` (11,549
lines, 35 `validate_*_exchange` entry points), a checked DTO pair in
`crates/riffdb-service/src/dto.rs` (11,622 lines, 192 public types), a
`*_from_proto` / `*_to_proto` pair in `crates/riffdb-api-grpc/src/conversion.rs`
(8,075 lines, 187 functions), a fixed-tool dispatch arm in
`crates/riffdb-api-mcp/src/service_backend.rs` (6,801 lines, 176 functions), a
typed method in `crates/riffdb-client-rust/src/client.rs` that re-imports the
same validation exchange, and a dispatch arm plus output encoder in
`crates/riffdb-cli/src/app.rs` (14,583 lines). The application facades are
expressed a seventh time by four string-templated generators in
`crates/riffdb-query-module/src/{generation,go_generation,python_generation}.rs`
that build source with 493 `write!`/`format!` calls; their infallible
`fmt::Write` results account for 552 of the workspace's roughly 1,000 panic-lint
hits.

The cost is measured, not inferred. Recent feature commits touched 55 files
across 10 crates (`ae5f9405`), 50 across 11 (`a9f92744`), 44 across 12
(`b72463e7`), and 39 across 9 (`2d161c89`). The most-changed files since June
are the mapping layers themselves: `store.rs`, `app.rs`, `generate_proto.rs`,
`symbolic_query.rs`, `generation.rs`, `conversion.rs`, and `daemon.rs`. The
same duplication produced drift: `docs/architecture/WP-682-PRE-CONSOLIDATION-FINDINGS.md`
records three inputs the socket driver rejected and the Python binding accepted,
with different error classes, until ADR-0148 routed both through one core.

The repository already has the discipline this record generalizes. SPEC 5.3
requires generated code to be checked in only when generation is deterministic
and CI verifies it; `scripts/check-generated` runs 22 `generate-*` scripts and
fails on any worktree change; `crates/riffdb-proto/tests/public_client_vectors.rs`,
`wire_vectors.rs`, and `crates/riffdb-api-mcp/fixtures/fixed-tool-conversion-goldens-v1.json`
already freeze public bytes. What is missing is one declaration per operation
from which the adapters are produced, so that the six layers cannot disagree.

## Decision

### 1. One registry declaration per public operation

A new leaf crate `riffdb-operation-registry`, depending only on
`riffdb-types`, holds exactly one declaration per public operation. A
declaration names the `ServiceOperationV1` variant, the Protobuf request and
response message identities, the service DTO request and result type paths,
every declared bound (encoded request and response bytes, page items,
collection counts, string bytes), the permitted `ServiceIngressKindV1` set, the
`CapabilityPermissionKind`, the idempotency class, the ADR-0118 redaction class
of every output field, the audiences, and a field map drawn from a closed
mapping vocabulary (identity, newtype, base64, enum tag, nested record, bounded
repeated). The registry is data. It grants no authority and performs no I/O.

### 2. Adapters are generated from the registry, never hand-written

A deterministic generator, `scripts/generate-operation-adapters`, reads the
registry and the compiled descriptor set under `fixtures/proto/descriptors`
and emits checked-in `src/generated/` modules for `riffdb-proto` (bounded
public-message validation), `riffdb-service` (DTO derivation helpers and the
closed operation table), `riffdb-api-grpc` (Protobuf-to-DTO and DTO-to-Protobuf
conversion), `riffdb-api-mcp` (fixed-tool schema and backend dispatch),
`riffdb-cli` (machine output envelope shapes), and `riffdb-client-rust` (the
typed method surface). Generation fails when a Protobuf field is unmapped,
mapped twice, or mapped to a DTO field of an incompatible mapping kind. The
ADR-0007 API-neutral service boundary, its hand-written operation semantics,
and the hand-written DTO types remain; only their adapters change owner.

### 3. Every replacement is proven byte-identical before it lands

Before a hand-written layer is replaced, its complete current output is frozen
as fixtures: every public wire vector, every MCP tool schema and fixed-tool
conversion golden, every `riffdb.cli.output/v1` envelope, and every generated
application catalog. The generated replacement reproduces those fixtures byte
for byte. No Protobuf package, tag, reserved range, MCP tool name, CLI envelope
field, public error code, or durable byte changes. Under ADR-0124 the change is
classified as internal with no domain version advance.

### 4. Language generators render one canonical model through templates

The Rust, Go, TypeScript, and Python application generators move from
`write!`-assembled strings to `minijinja` templates checked in under
`templates/generators/<language>/`, rendered with strict undefined variables,
autoescaping disabled, and deterministic iteration over ordered inputs. All four
render one canonical JSON generation model derived from the `QueryModule` and
`ContractBundle`; that model is itself a golden fixture. `minijinja` is chosen
over `askama` because it needs no procedural macro, keeps templates as reviewable
data files rather than compiled Rust, and is Apache-2.0 licensed under the existing
`deny.toml` allowlist with default features disabled.

### 5. One drift corpus runs every binding

`fixtures/driver/corpus-v1.json` becomes a v2 corpus covering every registry
operation reachable from the Rust client, the driver-host socket protocol, the
Python in-process path, and the generated Go and TypeScript facades, with
accepted and rejected entries for every declared bound. Each binding returns the
identical closed public error class for each entry, extending ADR-0148's
equivalence proof from value marshalling to the whole surface.

### 6. Adding one operation touches three hand-written places

After WP-753, adding a public operation hand-touches at most the Protobuf
source, the registry declaration together with the service implementation, and
the handbook. `scripts/check-three-places --base <rev>` enforces this: in a diff
that changes a `.proto` file or the registry, every changed file under the six
adapter crates is under a `src/generated/` directory, and `check-generated`
already fails when generated output is stale. An architecture test forbids
hand-written `validate_*_exchange` definitions and `ServiceOperationV1` dispatch
outside generated directories.

### 7. Decomposition is a by-product, never a package of its own

The five files over 10,000 lines on this path (`app.rs`, `dto.rs`,
`public_message.rs`, `conversion.rs`, `service_backend.rs`) shrink as their
generated replacements land. An architecture test holds each file on the
adapter path under 4,000 lines once WP-752 closes. No separate refactoring
package is authorized.

### 8. What does not change

Hosted MCP still constructs context only through
`RequestContext::from_authenticated_mcp_http`; gRPC and MCP still call only the
auth-owned `CredentialAuthenticator`; redaction under ADR-0027 and ADR-0118
still happens inside the service before any adapter sees a value; ADR-0148's one
driver protocol core remains the sole value marshaller for every binding. The
registry adds no operation, bound, permission, or surface.

## Options Considered

1. **Keep hand-written adapters with stronger review:** rejected. The fan-out
   and the WP-682 drift were produced under the current review discipline;
   review cannot make six copies agree by construction.
2. **Make Protobuf the sole source and generate the DTOs too:** rejected.
   ADR-0007 places checked semantic types (`NonZeroU32`, newtype identifiers,
   `Arc` plans) in the DTOs, and moving those semantics into Protobuf options
   would make the wire schema the semantic authority.
3. **Procedural derive on the DTOs carrying bounds in attributes:** deferred.
   It hides bounds inside attributes on the semantic crate, adds a proc-macro
   build edge to `riffdb-service`, and is harder to diff than data. The
   registry may adopt a derive for field maps later without changing this
   record's boundary.
4. **A data registry plus checked-in generated adapters:** chosen. It fits
   SPEC 5.3, `check-generated`, and the existing golden fixtures, and it makes
   disagreement between layers a generation failure.

## Consequences

- Adding or changing an operation touches three hand-written places; the
  adapter fan-out and the class of drift found by WP-682 disappear.
- Roughly 40,000 lines of hand-written mapping code become generated output,
  and the panic-lint noise from infallible string writes is removed from the
  generators.
- Cost: one new leaf crate, one generator, template files, a one-time fixture
  freeze for every existing operation, and the `minijinja` dependency.
- Deferred: generating kernel operations for Go, TypeScript, and Python beyond
  the application facades; any non-Protobuf transport; runtime template
  loading; changing any public identity.

## Compatibility

Public Protobuf packages, tags, reserved ranges, MCP tool names, the
`riffdb.cli.output/v1` envelope, public error codes, generated package layouts,
contract IR, RiffQL, query modules, and every durable byte are unchanged. The
frozen fixtures are additive. `release/version-topology-v1.json` gains no domain
and advances none. Existing generated application packages are byte-identical
after WP-753; later additive changes follow ADR-0124 as today.

## Security

The registry declares permission kinds and redaction classes but grants
nothing; every generated adapter calls the same service entry point that
authorizes per request at the ADR-0007 safe points. Redaction classes generated
into CLI and MCP output shapes make a secret output unprintable by construction
rather than by adapter discipline. Templates are data rendered at generation
time in CI; no template runs in `riffdbd`, `riffdb`, `riffdb-mcp`, or any
client, and no template receives application-controlled input. The generator
runs offline with no network access.

## Standing Design Tests

- **Interface safety (AGENTS.md boundary 11):** No. A generated adapter can
  express only an operation the registry declares, with the bounds it declares,
  and the architecture test and `check-three-places` forbid a hand-written
  adapter that could add an undeclared surface or skip a bound. The registry
  cannot express a wildcard operation, an unbounded field, or an opt-out from
  redaction, idempotency, or authorization.
- **Scale:** Not applicable to storage or memory. Generation is a build-time
  step over a registry whose size is the operation count; nothing here assumes
  co-located storage, single-node memory, or full-state rewrite.

## Testing

- `every_public_operation_has_exactly_one_registry_declaration` walks
  `ServiceOperationV1` and both service descriptor sets against the registry.
- `generated_adapters_reproduce_frozen_public_fixtures_byte_for_byte` compares
  generated validation, conversion, MCP, and CLI output with the frozen
  fixtures; `check-generated` guards freshness.
- `every_binding_returns_the_same_error_class_for_the_shared_corpus` runs the
  v2 corpus through all five bindings under `scripts/driver-conformance`.
- `template_generators_reproduce_golden_outputs_across_languages` renders the
  canonical model for every fixture module and compares each language output.
- `check-three-places` runs in CI against the merge base.
- `adapter_path_files_stay_under_the_decomposition_budget` pins the file
  budget. Existing `proto_public` and `riffql_parser` fuzz targets are
  unchanged; a registry property test asserts every Protobuf field is mapped
  exactly once.

## Requirements and Work Packages

- **Requirements:** `GEN-001` through `GEN-007`
- **Defines or blocks:** WP-751 through WP-753
- **Final evidence:** WP-753

## Decision Deadline

Exact acceptance is required before WP-751 adds the registry crate or moves
any validation bound out of `public_message.rs`, because the frozen fixtures
that prove byte identity are taken from the hand-written layers as they stand
at acceptance.
