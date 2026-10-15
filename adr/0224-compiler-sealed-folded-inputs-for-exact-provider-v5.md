---
adr: "0224"
title: Compiler-Sealed Folded Inputs For Exact Provider V5
status: accepted
tier: guarantee
date: 2026-09-11
accepted: "2026-09-11"
requires: [ADR-0172, ADR-0219, ADR-0222]
amends: [ADR-0172, ADR-0219, ADR-0222]
supersedes: []
requirements: [GOV-001, GOV-002, OQ-005, OQ-006, OQ-007, OQ-008, OQ-015, OQ-017, OQ-024, OQ-025, OQ-026, OQ-027, OQ-028, OQ-029]
packages: [WP-757]
obligations: []
review_triggers:
  - V5 framing, schema hash, scalar tags, comparison profiles, index algorithms, or codec bytes would change.
  - A transform would be inferred at runtime, omitted from full plan identity, caller-selected, or applied to authoritative state, policy evaluation, routing, or canonical keys.
  - Ambiguous profile declarations or conflicting uses of a field or parameter would be silently resolved instead of refused.
  - A predecessor provider, format alias, folded shadow entity field, query-time row fold, or compatibility fallback would survive.
  - Folded inputs would escape existing bounds or original output would be replaced with matched-form bytes.
  - Implementation would exceed the exact production paths or the separate acceptance and reconciliation sequence below.
---
# ADR-0224: Compiler-Sealed Folded Inputs For Exact Provider V5

## Context

Accepted ADR-0172 requires the same frozen Unicode 17.0.0 NFKC plus full
non-Turkic case fold in operational indexes and exact-text providers. At
recovery checkpoint `9310f076`, its two provider-profile tests pass, but folded
provider matching still depends on `ExactTextPartitionIndexV1`. The named
exact-text compiler requires a binary text key; preserving that reachability
gap or deleting the test does not discharge ADR-0172.

ADR-0219 requires genuine sole-current V5, deletion of V1 through V4, and exact
V5 bytes and comparison profiles. V5 stores ordinary string cells and original
output records, and already binds the complete query-plan hash separately from
its predicate-program hash. This permits a compiler-owned input representation
outside the frozen V2 predicate program; it does not authorize an unbound fold
or a new comparison tag disguised as existing V5.

## Decision

1. Preserve ADR-0172's folded exact matching through compiler-sealed
   **matched-form inputs** to genuine `ExactPredicateProgramV2` and V5.
   The physical string comparator remains `BinaryUtf8`: it compares the
   declared matched form bytewise. No V5 profile, scalar tag, field, framing,
   schema hash, reader, writer, or algorithm is added or changed. For identical
   V5 binding, program, rows and output, encoded bytes remain exactly identical.

2. Add one immutable compiler-owned `ExactMatchedFormBindingV1`, distinct from
   provider state and never supplied by an application. It names the bound V2
   program identity, entity identity, and sorted, duplicate-free maps from
   referenced field IDs and value-slot ordinals to `Identity` or
   `UnicodeFoldV1`. The latter means exactly ADR-0172's existing pinned
   transform; no configurable locale, table version, expression or plug-in
   exists. Non-string cells have only Identity. Every referenced field and
   parameter is covered, including identity cases; unknown or extra entries
   and incompatible scalar/set profiles are invalid.

3. The compiler resolves profiles from contract text-key declarations, never
   from request values, index statistics or runtime availability. A field with
   only declared binary text-key profiles uses Identity; one with only
   `unicode_fold_v1` uses UnicodeFoldV1. An ordinary exact scalar field with
   no text-key declaration keeps its existing comparison. Competing binary
   and folded declarations for a referenced string field are a source-spanned
   ambiguity refusal. This is an explicit new compile-time refusal, not
   permission to choose whichever profile is cheaper or available.

4. One referenced field has one matched form throughout the complete predicate,
   optional-presence family and order. Each parameter slot must have the same
   profile at every use; conflicting uses fail compilation. Partition-route
   fields and canonical entity-key tie-breaker fields remain Identity;
   a folded declaration requiring those same cells to serve both roles is
   refused. This is a named POC limitation, not permission to fold routing or
   invent synthetic authoritative/shadow fields. Ordinary operational text-key
   behavior is unchanged. Null/missing states and presence selection are
   unchanged; PresentOnly still requires the complete installed-lineage proof.

5. Own the checked binding and shared bounded conversion operations in
   `crates/riffdb-query-ir/src/exact_predicate.rs`, outside the canonical V2
   program. Compiler owners construct it from resolved declarations; module
   owners verify its exact program/field/slot coverage and seal it into each
   exact-text, filtered, predicate, independent-order and nullable query.
   Do not copy lowering, dispatch through a different family, or infer the
   binding later from whatever catalog happens to be installed.

6. Canonical binding bytes are `RXMF`, big-endian u16 version 1, the 32-byte
   V2 program hash, u32 entity ID, u16 field count, sorted (u32 field ID, u8
   profile) entries, u16 slot count, then sorted (u16 slot, u8 profile) entries.
   Profiles are exactly Identity=1 and UnicodeFoldV1=2. Reject trailing bytes,
   noncanonical maps, unknown identities, and size above 65,536 bytes before
   allocation; counts also obey the bound program's existing ceilings. Its complete
   bytes are included in the owning compiled-query identity, not merely a
   diagnostic or optional checksum. The provider's existing full `plan` hash
   thereby binds the input representation; its `program` hash still binds
   genuine V2. Equal V2 program hashes alone NEVER establish interchangeable
   folded/binary plans. The descriptor still pins physical V5 byte comparison;
   only the complete compiled query declares normalization. No extra manifest
   or transform tag is stored in V5.

7. The three owning compiled-query encodings advance uniformly: exact-text
   RXCQ V2 becomes V3; exact-predicate and nullable exact-predicate container
   versions 1 become 2. Identity-only queries use the same current encoding,
   with an explicit Identity binding, rather than a least-sufficient branch.
   Register the binding and both predicate containers as exact executable
   domains if absent, and reconcile the exact-text domain. Old container
   readers/artifacts are deleted and refused, not reinterpreted. The outer
   query-module V18 envelope, general query IR V18, RiffQL V14 and application
   operations V5 remain governed by ADR-0216; no grammar or dispatcher changes.
   This explicitly amends ADR-0219/0222's artifact-rotation restriction only
   for the new sealed binding and these owning encodings.

8. Background rebuild and incremental inputs evaluate authorization on original
   authoritative records first. The checked binding then prepares only the
   bounded admitted provider cells. V5 indexes and checkpoints the matched-form
   cells; its existing canonical output records and canonical entity keys
   retain original values. Filter/order cells use their declared matched form,
   but no application write, stored entity, policy input or route is transformed.
   Per-row transformation during query execution or hydration is forbidden.

9. The existing service materializer uses the shared checked conversion once
   per scalar/set slot, before matched-needle length/emptiness validation.
   Fold sets before canonical sorting/deduplication; null/missing stay absent.
   It attaches an opaque prepared-values witness binding the exact matched-form
   binding identity to those values in the existing internal request structs.
   Only checked conversion can construct the witness. The server verifies it
   against the compiled query and never re-folds request values; raw, substituted
   or differently prepared values refuse. Keep original parameters for existing
   authorization/cursor rules. No new operation, trait method, public schema,
   profile parameter, version selector or caller bypass is added.

10. Check original input bounds before allocation and charge checked worst-case
    expansion using the existing factor 18. Apply the existing pinned function
    only within that charge, then enforce every existing matched-value,
    needle, collection, row, state-byte, work and output ceiling on the actual
    result. No limit increases, unchecked multiplication, hidden cache,
    uncharged scan or population-sized temporary is permitted. Overflow or
    excess is the existing safe bound refusal before publication/mutation.

11. Recovery decodes only genuine V5 and requires its full plan, V2 program,
    descriptor, policy, partition, history, generation and epoch bindings to
    match the expected compiled query and readiness proof. Stored provider
    cells are already prepared: recovery never re-folds or infers their profile.
    A wrong matched-form binding means a different plan and refuses adoption;
    a new generation comes only from the accepted bounded authoritative rebuild.
    Fold collisions retain every distinct entity and original output; exact
    count, ordinal selection, key tie-breaks and null placement remain exact.

12. Amend ADR-0172's provider-format compatibility paragraph only for epoch two:
    folded behavior is preserved by Decisions 1–11, not by retaining its V1
    implementation or adding a second provider codec. Delete V1–V4 providers,
    aliases, constants, locators and readable fixtures. Retarget its semantic
    tests to real compiled V5 execution; testing the fold helper alone is
    insufficient. OBL-0219-1 and its exact proof name remain mandatory.

13. Retain ADR-0219's eight and ADR-0222's two production paths. Add exactly
    `crates/riffdb-service/src/symbolic_query.rs` for Decision 9 materialization,
    `crates/riffdb-service/src/ports.rs` only for the internal prepared-values
    witness carriage/checks, and `crates/riffdb-projection/src/lib.rs` only to
    delete retired `mod exact_text`/`pub use exact_text::*` when no current export
    remains. No fabricated export or wrapper retains that module. Module
    `src/lib.rs`, grammar, storage, transaction behavior, dependencies and Unicode
    tables remain excluded. Existing semantic owners are not copied or moved.

14. This proposal changes only this record and its generated index. After exact
    human acceptance, first make a separate WP-757-only path-authority commit
    adding exactly `adr/0172-case-insensitive-text-matching.md` to its paths.
    Then separately reconcile that ADR, SPEC and WP-757's requirements,
    deliverables, required ADRs, obligations and review triggers. Reconcile
    topology/artifacts with implementation; independently derive and review
    every resulting plan/module/lock/cursor/fixture hash. Preserve ADR-0219's
    historical hash pair without assigning new causality. No implementation
    authority or package closure is inferred from a proposed record.

## Options considered

- Retaining/relabeling V1 violates retirement; dropping fold matching or
  testing only the transform violates ADR-0172's two-consumer requirement.
- Adding a fold tag to V5 silently changes the frozen format. A new provider
  format would require a different, broader decision; it is not chosen here.
- Compiler-sealed prepared inputs use V5's existing full-plan identity and
  string-cell representation. The explicit container rotations pay for that
  new authority without changing the physical V5 contract.

## Consequences

- Declared folded exact queries become executable without adapter shadow data;
  original output and the frozen Unicode transform remain shared.
- Mixed-profile and key/route-role ambiguities are explicit POC refusals.
  New compiler diagnostics and the whole affected artifact campaign need review.
- Binary truth and V5 codec vectors remain unchanged; compiled-query hashes
  intentionally rotate, including identity-only bindings. No release,
  publication, benchmark or external-adapter gate is waived.

## Standing design tests

- **Interface safety:** applications declare contract text-key semantics, but
  cannot submit transforms, omit the sealed binding, substitute state, bypass
  policy or choose a fallback. Unsupported combinations fail before execution;
  no unsafe operation or silent guarantee opt-out is expressible.
- **Scale:** conversion is bounded per admitted rebuild row and request slot;
  no co-located-storage assumption, full-state request rewrite, remote hydration
  or second authoritative data representation is introduced.

## Checks

- `epoch_two_folded_exact_inputs_are_sealed_and_v5_bytes_remain_exact` proves
  real compiler/module/server V5 execution for every exact family, four text
  operators, equality/set/range/complement, folded ordering/collisions,
  original output, null placement, both directions, optional filters and pages.
- Include independent Unicode pairs (NFKC ligatures, sharp-s, dotted-I), binary
  nonmatches, worst-case expansion, post-fold bounds/empty needles, conflicting
  field/slot profiles and ambiguity diagnostics with spans.
- Prove unprepared/different-witness refusal and full-plan mismatch despite
  equal V2 hashes, checkpoint/restart,
  stale policy/history/epoch, mutation atomicity, and exact frozen V5 vectors.
  Include a successful nonempty authoritative rebuild and process crash recovery.
- Run OBL-0219-1, source/fixture retirement, topology, requirement/obligation,
  allowed-path and handbook checks, full acceptance and `ci-all`; review exact
  container and downstream artifact diffs before merge.
