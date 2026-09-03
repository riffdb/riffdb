---
adr: 0189
title: Compiler-Sealed Operational Index Descriptor Reconstruction
status: accepted
tier: guarantee
date: 2026-09-03
accepted: "2026-09-03 (maintainer, in session)"
requires: [ADR-0051, ADR-0053, ADR-0108, ADR-0124, ADR-0150, ADR-0172]
amends:
  - ADR-0150 section 2 only by defining the permitted reconstruction of an operational physical profile already present in the exact contract bundle
  - ADR-0172 only by carrying its accepted unicode_fold_v1 transform into ordinary physical parameter lowering
supersedes: []
requirements: [OQ-032, OQ-033, OQ-034, OQ-037, OQ-038, OQ-043]
packages: [WP-688]
obligations:
  - id: OBL-0189-1
    package: WP-688
    proof: operational_descriptor_reconstruction_preserves_canonical_plan_bytes_and_hash
    says: Reconstructing an operational index descriptor leaves existing query IR, plan, module, lock, cursor, and generated-surface bytes and identities unchanged.
  - id: OBL-0189-2
    package: WP-688
    proof: operational_descriptor_refuses_mismatched_bundle_index_or_schema_before_storage
    says: A wrong bundle, entity, index identity, symbolic name, field order, encoding vector, physical arity, key schema, or component codec is refused before storage access.
  - id: OBL-0189-3
    package: WP-688
    proof: unicode_fold_exact_predicate_uses_the_declared_physical_profile
    says: Ordinary unicode_fold_v1 predicate values use the same frozen transform as index maintenance before physical range construction.
review_triggers:
  - The descriptor or any of its fields would enter canonical query IR, plan, module, lock, cursor, wire, generated-surface, or durable bytes.
  - Reconstruction would use an active bundle, symbolic name, key codec, or runtime registry without proving the exact bundle and stable index identity.
  - Storage would receive contract IR, a profile callback, or authority to choose a transform.
  - A new encoding, text profile, operator role, comparison meaning, fallback, or runtime access-path choice would be introduced.
  - Profile validation, normalization, or range construction would move into a per-row or per-storage-operation path.
---
# ADR-0189: Compiler-Sealed Operational Index Descriptor Reconstruction

## Context

ADR-0150 requires one compiler-sealed physical profile for each ordinary index
step and permits that profile to be reconstructed when the existing executable
artifact already carries enough exact facts. `QueryAccessStep` currently retains
the selected index ID and key schema, while canonical plan bytes bind the exact
contract bundle hash, symbolic index, field order, direction, predicates, and
source. The exact bundle independently records each index field's
`IndexFieldEncodingV1`.

The key schema alone is not sufficient. Both `binary_utf8_v1` and
`unicode_fold_v1` use an `OrderedBytes` physical component. Current parameter
lowering therefore treats either codec as raw UTF-8. Index maintenance instead
applies ADR-0172's frozen Unicode fold, so `"STRASSE"` fails to select a stored
`"Straße"` row. Adding the profile to canonical query IR would duplicate a fact
already sealed by the exact bundle and change plan, module, lock, and cursor
identities unnecessarily.

## Decision

### 1. Reconstruct one private operational index descriptor

For each ordinary index access, the compiler or trusted catalog/module loader
constructs one private `OperationalIndexDescriptorV1` from:

- the exact validated contract bundle named by the query program;
- the step's compiler-retained entity and index IDs;
- the step's symbolic entity, index, and logical field order; and
- the step's retained partition, entity-key, and index-key schemas.

The stable entity and index IDs select the bundle declarations. Symbolic names
are cross-checks and are never an alternative lookup authority. The descriptor
records, for every logical index field, its exact `IndexFieldEncodingV1`, its
physical component start and width, and the corresponding checked physical
component schema.

The descriptor is bounded, process-local, nonserializable, and compiler-sealed.
It grants no storage, authorization, policy, cursor, or mutation capability.
The executor receives the completed descriptor with the step; it does not
receive a bundle or discover an access path at request time.

### 2. Validate the complete reconstruction before admission

Construction succeeds only when all of these facts agree exactly:

1. the bundle's recomputed lineage, version, and bundle hash equal the query's
   exact contract reference;
2. the entity ID resolves to the step's symbolic entity and retained entity-key
   and partition-key schemas;
3. the index ID resolves under that entity to the step's symbolic index;
4. the declared logical field IDs and names equal the step's access-field order;
5. the encoding vector has exactly one entry per logical field;
6. canonical and text-key encodings occupy one physical component, while a
   presence encoding occupies its exact discriminator and payload pair;
7. logical field types, physical component types, codecs, enum domains, and
   maximum payload bounds satisfy the bundle's checked index declaration; and
8. the complete reconstructed physical sequence equals the retained index-key
   schema, including its owning entity and index IDs and embedded entity key.

A mismatch, missing descriptor, unknown profile, excess arity, trailing
component, or inconsistent bound is not repaired or inferred. Compilation fails
with a bounded internal diagnostic, or an already persisted artifact returns
`RDB-QUERY-0102` (`QueryUnavailable`) before storage access with the existing
refresh/recompile remedy. No nearby active bundle, name-only lookup, raw key
codec, backend capability, or caller value may substitute.

### 3. Select logical-to-physical lowering only from the descriptor

Per-step/request parameter lowering consumes the descriptor once:

- `Canonical` uses the existing canonical scalar component encoding;
- `Presence` uses the accepted missing, null, or present discriminator and
  checked payload rules;
- `TextKey(BinaryUtf8)` uses exact canonical UTF-8 bytes; and
- `TextKey(UnicodeFold)` uses ADR-0172's single frozen
  `unicode_fold_v1` implementation.

Index maintenance and predicate lowering call the same profile implementation.
The transformed value is checked against the descriptor's post-transform
physical byte bound before a range is formed. Transformation, validation, range
construction, sorting, and deduplication occur once per bounded step/request,
never per row, page item, point read, or storage operation.

Storage continues to receive only the immutable physical range schedule. It
does not receive contract IR, text-profile identity, a transform callback, or a
choice among encodings.

### 4. Preserve canonical artifacts and identities exactly

`OperationalIndexDescriptorV1` is excluded from canonical program encoding and
every public or durable identity. Existing query IR, plan, module, source, lock,
role, generated binding, cursor, storage key, index row, bundle, and protocol
bytes remain byte-identical.

This is valid because the descriptor introduces no new semantic choice. The
canonical program already binds the exact bundle and symbolic access, while the
checked in-process step retains the corresponding stable index ID. The exact
bundle already includes the index encoding vector and physical key schema.
Reconstruction merely recovers those already-bound facts and proves they agree.

Tests freeze canonical bytes and hashes before and after descriptor attachment.
Any implementation that cannot reproduce the descriptor from those exact facts
must stop for an ADR-0124 successor rather than encode an unreviewed side
channel.

### 5. Correct execution without broadening the matrix

The repair activates no new language, encoding, profile, role, or role
combination. It implements only cells already accepted by ADR-0150 as amended
by ADR-0172. In particular, `unicode_fold_v1` equality, membership, range,
complement, prefix, and order retain ADR-0172's folded-byte meanings and all
ADR-0150 shape restrictions.

Every logical predicate must still be consumed physically before page and
continuation formation. Unsupported profiles, incomplete orders, residual
predicates, multiple branching dimensions, overlapping unions, skip scans,
intersections, adaptive planning, bitmap bridges, and caller-selected fallback
remain refused.

## Options considered

1. **Infer the profile from `KeyComponentCodecV1`:** rejected because
   `binary_utf8_v1` and `unicode_fold_v1` deliberately share `OrderedBytes`.
2. **Add the profile to canonical query IR:** rejected because the exact bundle
   already owns it and duplicating it would require successor plan, module,
   lock, cursor, and generated identities with an agreement invariant.
3. **Give the executor or storage the contract bundle:** rejected because it
   would move access-path discovery and contract interpretation below the
   compiler boundary.
4. **Reconstruct and cross-check one private descriptor:** chosen because it
   restores the already accepted semantics while preserving every existing
   artifact and authority boundary.

## Consequences

- Ordinary physical lowering can distinguish canonical, presence-aware,
  binary-text, and Unicode-fold components without inspecting only their key
  codecs.
- Query step construction gains one bounded validation and one internal
  descriptor.
- Persisted artifacts that cannot reproduce the exact descriptor fail closed
  before storage rather than executing with raw or approximate bytes.
- No public API, durable format, canonical IR, cursor, generated binding, or
  topology identity changes.
- New profiles and physical meanings still require their own accepted ADR and
  compatibility classification.

## Standing design tests

- **Interface safety:** applications continue to submit only typed bounded
  values to finite compiled operations. They cannot name or alter an encoding,
  profile, descriptor, index, physical range, comparator, fallback, or
  reconstruction source. A missing or inconsistent proof fails before storage.
- **Scale:** descriptor size is bounded by the declared index field and physical
  component ceilings. It is built once per compiled or loaded step, and
  parameter transformation and range construction remain bounded once per
  request. No history scan, database-wide state, per-row validation, or
  unbounded cache is introduced.

## Compatibility

This record changes no canonical or durable bytes. Existing program bytes and
identities remain authoritative and receive the descriptor only after exact
bundle and index validation. The `unicode_fold_v1` behavior is the semantics
already accepted by ADR-0172; matching folded stored values with identically
folded submitted values corrects the implementation rather than reinterpreting
the plan.

An older runtime presented with artifacts it already understands behaves as it
did before. A repaired runtime refuses an inconsistent or unreconstructible
artifact before storage. Decoder retirement, a new text profile, a key-codec
change, or any inability to reconstruct from the current exact facts requires a
separate ADR-0124 ceremony.

## Security

The descriptor contains schema and profile identities only, never submitted
values, credentials, policy facts, rows, or storage handles. Submitted text and
transformed bytes remain redacted from diagnostics, logs, metrics, tracing, MCP
text, and public errors.

Wrong-bundle, wrong-index, schema, codec, bound, and profile mismatches fail
closed. There is no raw-byte fallback, active-bundle fallback, storage-local
profile selection, or partial page/cursor release.
