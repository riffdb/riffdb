---
adr: 0191
title: Canonical Aggregate Collection Preflight and Stable Error Mapping
status: accepted
tier: guarantee
date: 2026-09-03
accepted: "2026-09-03"
requires: [ADR-0003, ADR-0004, ADR-0005, ADR-0011, ADR-0012, ADR-0031,
  ADR-0041, ADR-0055, ADR-0056, ADR-0059, ADR-0064, ADR-0106, ADR-0107,
  ADR-0124, ADR-0129, ADR-0147, ADR-0148]
amends:
  - ADR-0147 section 4 by freezing exact typed-facade and exact-bundle CLI preflight while leaving type-incomplete raw CLI and MCP canonical sizing authoritative in the Rust service
  - ADR-0148 section 2 only by permitting compiler-generated advisory facade preflight while the shared Rust protocol core remains authoritative for admission and public-error mapping
  - SPEC BLK-019 by distinguishing exact typed local preflight from structural raw CLI and MCP validation without weakening authoritative pre-effect enforcement
supersedes: []
requirements: [BLK-019, BLK-020, BLK-021, AAA-001, AAA-004, AAA-005, AAA-010]
packages: [WP-679]
obligations:
  - id: OBL-0191-1
    package: WP-679
    proof: aggregate_collection_annotation_is_exact_bounded_and_array_only
    says: Every generated command-input JSON Schema for an aggregate-constrained collection publishes the one exact positive bounded array annotation and no second aggregate-budget keyword.
  - id: OBL-0191-2
    package: WP-679
    proof: generated_aggregate_preflight_causes_are_closed_and_transport_free
    says: Rust, Go, TypeScript, and Python typed aggregate-budget preflight returns only the three closed budget causes at the exact collection or indexed-leaf path before emitting that command request, while ordinary shape failures remain unchanged.
  - id: OBL-0191-3
    package: WP-679
    proof: aggregate_budget_service_errors_have_exact_paths_and_codes
    says: Authoritative below-minimum, above-maximum, individual-value, and aggregate-byte refusals use the exact existing validation code and numeric path mapping before effectful evaluation.
  - id: OBL-0191-4
    package: WP-679
    proof: expanded_graph_resource_limit_is_terminal_without_application_effects
    says: Concrete expanded-graph overflow uses the existing command-execution resource-limit failure and produces no application mutation, sequence, event, outbox intent, declared outcome, or provenance.
  - id: OBL-0191-5
    package: WP-679
    proof: aggregate_budget_public_and_durable_identities_are_unchanged
    says: Operation schemas and hashes, plan and operation identities, Protobuf and public-error registries, driver protocol, and durable codecs and fixtures remain byte-identical while reviewed generated facade sources and their artifact hashes rotate normally.
  - id: OBL-0191-6
    package: WP-679
    proof: aggregate_budget_cross_language_error_observations_are_equal
    says: Typed generated facades agree on normalized local budget cause, precedence, and path; exact-bundle CLI agrees on refusal without a request; and service and gRPC agree on the authoritative RDB pair and numeric path rendered normally by CLI and MCP.
review_triggers:
  - The annotation name, integer domain, inclusive ceiling, array-only placement, canonical measurement, or excluded framing would change.
  - A new PublicError, RDB code, Protobuf field, operation-schema hash, plan or operation identity, driver protocol field, CLI/MCP envelope identity, or durable format would be required.
  - Raw CLI or MCP would estimate canonical size from type-incomplete JSON Schema, or local preflight would skip authoritative service validation.
  - Budget causes would use another precedence, path, public mapping, or effect boundary, or ordinary type and shape failures would be relabeled.
  - Canonical sizing would be repeated per authorization atom, retry, mutation, durable transition, or graph row, or allocate from an untrusted claimed length.
  - WP-679 would need another dependency, a path outside its present scope, or a change to command identity, ordering, atomicity, authorization, or durability.
---
# ADR-0191: Canonical Aggregate Collection Preflight and Stable Error Mapping

## Context

ADR-0147 requires one canonical aggregate-element-byte bound and distinct
cardinality, individual-value, aggregate-input, and complete-graph failures. It
does not freeze the JSON Schema keyword, generated local causes, or mapping into
the existing validation and execution-failure registry. WP-679 cannot choose
those public semantics without this record.

Typed generated facades and the Rust service possess the exact contract types
needed for ADR-0011 sizing. Raw CLI input and MCP dynamic-tool JSON Schema do
not: several canonical types intentionally share the same JSON representation.
Estimating from that schema could disagree with the authoritative bytes. Exact
local feedback must therefore be limited to a surface holding the exact type
proof, while every request remains authoritatively checked before effects.

## Decision

### 1. Classify the complete change as guarantee tier

The annotation and generated local causes are surface changes. WP-679 is
guarantee tier because it also fixes pre-effect rejection and deterministic
runtime failure mapping in guarantee paths. Preserving protocol and durable
identities does not lower the maximum tier.

### 2. Freeze one array annotation and byte definition

The exact keyword is `x-riffdb-aggregateCanonicalElementBytes`. It appears only
on the array for a collection expanded by a compiled bulk command carrying
ADR-0147's constraint. Its value is an integer in inclusive range 1 through
16,777,216. Another domain, placement, aggregate keyword, clamp, rounding,
fallback, or caller interpretation is invalid.

The value bounds the checked sum of each typed-decoded element's complete
ADR-0011 canonical value document in submitted order, including its version,
tag, length, record, optional, and nested framing. It excludes enclosing-list
version, tag, count, and length framing and every other input. It never means
JSON, Protobuf, driver-frame, compressed, host-object, allocated, or graph size.

The bound replaces no list minimum or maximum, leaf or nested-value limit,
request-envelope limit, or 16-MiB graph ceiling. Equality at each maximum is
valid. Non-RiffDB validators may treat the keyword as annotation; authoritative
enforcement remains mandatory.

### 3. Use closed typed-local budget causes and bounded precedence

Rust, Go, TypeScript, and Python typed generated facade preflight uses exactly
three aggregate-budget causes: `collection_count`, `individual_value_bytes`,
and `aggregate_canonical_element_bytes`. Existing missing-field, record-shape,
UUID, enum, timestamp, and other type failures keep their existing local class.
These local failures are not `PublicError`, carry no RDB code, cannot be decoded
from a server response, and claim no incident, retry, durability, or authority.

After existing outer command-shape checks, preflight validates the constrained
collection's minimum and maximum count, then each element and nested leaf in
submitted order, and only then the checked aggregate sum. Within that
collection, count wins over individual and aggregate failure, and the first
invalid indexed leaf wins over aggregate overflow. Checked-add failure is an
aggregate cause. Unrelated field ordering and multi-issue behavior do not move.

Count and aggregate local paths name the symbolic collection. Individual paths
name collection, zero-based index, and exact nested leaf. Target-native paths
must normalize through the exact schema to that meaning and remain bounded,
deterministic, and value-free. Preflight completes before emitting or using a
transport request for that command; an already-open long-lived connection is
permitted.

An exact-bundle CLI command performs the same exact accept/refuse preflight
before its request while retaining the existing bounded CLI local envelope.
Raw CLI performs its existing generic bounded-input validation. MCP publishes
the applicable structural/count schema and aggregate annotation and performs
ordinary local schema validation. Neither estimates canonical bytes without an
exact type proof. Exact aggregate sizing for those paths occurs in the shared
Rust service, and normal rendering preserves its authoritative public error.

### 4. Map authoritative failures into the existing registry

The application service repeats typed decoding and validation and maps:

| Cause | Existing kind / application code | Detail | Numeric path |
|---|---|---|---|
| count below minimum | `validation_failed` / `RDB-INPUT-0101` | `out_of_range` | collection |
| count above maximum | `validation_failed` / `RDB-INPUT-0101` | `too_many_items` | collection |
| individual value bytes | `validation_failed` / `RDB-INPUT-0101` | `too_long` | collection, index, leaf |
| aggregate canonical bytes | `validation_failed` / `RDB-INPUT-0101` | `too_long` | collection |
| complete expanded graph | `command_execution_failed` / `RDB-COMMAND-0102` | `resource_limit` | none |

No new public kind, code, detail, status, suggested fix, or prose is added. A
graph limit has no submitted-value path. Public paths use existing FieldId and
ListIndex segments; equality with local symbolic paths is semantic after exact
schema resolution, not byte equality.

Errors remain bounded, static, authorization-filtered, and free of submitted
bytes, element contents, canonical fragments, hidden graph shape, credentials,
or internal source. They need not expose an observed byte count.

### 5. Preserve service and runtime authority

The service validates count and individual values, canonicalizes typed
elements, and recomputes the aggregate with checked arithmetic before
authorization-dependent evaluation, conflict acquisition, runtime execution,
staging, journaling, or application sequence. Generated and exact-bundle CLI
preflight remains advisory; raw CLI and MCP gain no local type authority.

The deterministic runtime validates the concrete expanded graph before
coordinator submission. Overflow follows ADR-0012's existing `ResourceLimit`
terminal admission and revalidation rules. It may resolve a pending admission
but creates no application mutation, `CommitSequence`, declared outcome, event,
outbox intent, application commit, or command provenance. No layer splits,
truncates, retries a subset, or returns a partial element result.

Canonical length is one bounded pass over count- and value-bounded input and may
reuse that layer's checked typed decode or length counter. It is not recomputed
per authorization atom, conflict, retry, mutation, durable row, or copied graph
occurrence. Runtime graph validation is a distinct bound.

### 6. Preserve protocol, operation, and durable identities

This decision adds no Protobuf field or enum, public-error registry member,
operation-schema byte or hash, plan or operation identity, driver protocol,
grammar/IR/bundle version, storage key, durable envelope, terminal variant, or
topology identity. Existing RDB codes, status mapping, validation details, and
execution `ResourceLimit` are reused exactly.

Generated facade source changes are expected to expose the three local budget
causes. Reviewed regenerated source bytes, generated artifact content hashes,
and the corresponding application-lock artifact entries rotate under the
existing exact-generation ceremony. The exact operation schemas, their hashes,
contract and plan identities, and durable compatibility fixtures do not rotate.
Existing generated applications remain service-safe; callers regenerating Rust
may need to update exhaustive matching of the pre-1.0 local error enum.

An implementation needing another identity change must stop for ADR-0124. It
must not hide one behind regenerated fixtures.

### 7. Reconcile WP-679 without widening its paths

WP-679 becomes guarantee tier, depends on completed WP-682, and requires this
record plus ADR-0011, ADR-0041, ADR-0056, ADR-0064, ADR-0124, and ADR-0148. Its
existing paths cover the generators, clients, CLI, MCP, service, runtime,
fixtures, tests, scripts, and handbook. No implementation path is added;
`crates/riffdb-errors/**` remains outside scope and Proto paths are negative
identity evidence only. Governance acceptance remains a separate change.

The six obligations become named semantic tests in existing WP-679 acceptance.
Generated-fixture changes require human review; Proto, topology, requirement,
workspace, handbook, format, and Clippy gates remain mandatory.

## Options considered

1. Add canonical type metadata to MCP Schema or protocol: rejected because it
   rotates an identity merely to duplicate service authority.
2. Estimate from JSON representation: rejected because equal JSON shapes can
   have different canonical lengths. Typed local preflight plus authoritative
   service enforcement is chosen.

## Consequences

Typed callers receive precise local feedback without granting raw JSON surfaces
canonical type authority. Raw CLI and MCP may use transport for an aggregate
overflow, but the service refuses it before authoritative effects. Generated
source and lock artifact hashes rotate; operation, wire, and durable identities
do not. Atomicity, idempotency, authorization, ordering, and recovery remain.

## Standing design tests

- **Interface safety:** callers submit only one compiled command's values. They
  cannot set a bound, cause, path, graph ceiling, split policy, or fallback;
  local preflight never substitutes for authoritative service validation.
- **Scale:** count, value, aggregate, nesting, path, request, and graph bounds
  remain independent. Each capable layer performs at most one linear checked
  aggregate pass; work does not grow with authorization, retries, history, or
  database population.

## Compatibility

Public operation schemas, protocol/error registries, plans, and durable bytes
remain exact. Regenerated pre-1.0 facades gain closed local budget evidence and
rotate only their reviewed source/artifact hashes. Old generated clients remain
compatible because the service is authoritative. Raw CLI and MCP keep their
existing schema and envelope identities and render authoritative errors normally.

## Security

Local causes and public errors carry only closed classes and bounded paths.
Submitted values, canonical fragments, credentials, hidden schema, and graph
shape remain redacted. Structural local validation grants no type, operation,
authorization, storage, or effect authority.

## Checks

- `aggregate_collection_annotation_is_exact_bounded_and_array_only`
- `generated_aggregate_preflight_causes_are_closed_and_transport_free`
- `aggregate_budget_service_errors_have_exact_paths_and_codes`
- `expanded_graph_resource_limit_is_terminal_without_application_effects`
- `aggregate_budget_public_and_durable_identities_are_unchanged`
- `aggregate_budget_cross_language_error_observations_are_equal`
- `./scripts/check-generated`, `./scripts/generate-proto --check`,
  `./scripts/check-version-topology`, and `./scripts/check-requirement-coverage`
