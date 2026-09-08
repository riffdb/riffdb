---
adr: "0219"
title: Epoch-Two Sole-Current Exact-Provider State
status: accepted
tier: guarantee
date: 2026-09-07
accepted: "2026-09-08"
requires: [ADR-0124, ADR-0131, ADR-0134, ADR-0145, ADR-0181, ADR-0185,
  ADR-0204, ADR-0216]
amends: [ADR-0131, ADR-0134, ADR-0145, ADR-0216]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, OQ-005, OQ-006, OQ-015, OQ-017,
  OQ-019, OQ-020, OQ-021, OQ-022, OQ-024, OQ-025, OQ-026, OQ-027,
  OQ-028, OQ-029, OQ-030, VER-001, VER-002, VER-003, VER-004, VER-008]
packages: [WP-757]
obligations:
  - id: OBL-0219-1
    package: WP-757
    proof: epoch_two_exact_provider_state_is_sole_v5_and_plan_rotations_are_exact
    says: Epoch two admits only exact-provider state V5, removes and refuses every V1 through V4 source and artifact, and reviews the exact resulting plan, module, fixture, and hash rotations without changing exact-provider semantics or V5 bytes.
review_triggers:
  - A V1 through V4 exact-provider state constant, reader, writer, fixture, alias, descriptor, source locator, or dispatch would remain or be reinterpreted.
  - V5 checkpoint bytes, comparison, null placement, count, ordinal, policy, epoch, readiness, retention, or refusal semantics would change.
  - The historical main or pre-provider integration plan hash would differ from the exact commit-bound pair in Decision 8 without explanation, causality would be assigned to that difference, or a final combined plan/module/fixture rotation would be updated without reviewed derivation.
  - ADR-0216 or OBL-0185-1 retirement would not precede the query identity and fixture rotation required here.
  - An application, agent, transport, storage, migration, cursor, provider-selection, or compatibility surface would change.
  - WP-757 would edit the amended ADRs before a separate path-authority commit or combine path authority with reconciliation or implementation.
---
# ADR-0219: Epoch-Two Sole-Current Exact-Provider State

## Context

ADR-0181 and ADR-0204 require one readable, writable, and current identity per
epoch-two topology domain, but ADR-0131 retains exact-text provider-state V1,
ADR-0134 adds V4 while retaining V1 through V3, and ADR-0145 keeps V4 readable
and writable beside nullable-order V5. Those additive histories are truthful
epoch-one evidence, but retaining their constants, descriptors, codecs, and
fixtures in WP-757 would violate the accepted pre-external reset.

ADR-0185 OBL-0185-1 protects a main plan hash, while the clean integration tip
already produces a different hash before this provider retirement begins.
ADR-0216 proposes retiring that least-sufficient epoch-one obligation and
making V14/V18/V18 sole-current query identities. WP-757 cannot attribute the
existing difference to V5, silently update the proof, or perform provider-state
retirement before that proposal is exactly accepted.

## Decision

1. After exact acceptance of ADR-0216 and this record, WP-757 makes
   `durable.exact_text_provider_state` readable, writable, current, and active
   only at identity `5`. Its writer policy remains `single_current`; V1 through
   V4 are absent from every live lifecycle set and permanently reserved.

2. Every exact-text, filtered exact-text, exact-predicate, independent-order,
   and nullable-order family is canonically lowered to
   `ExactPredicateProgramV2`, and every compiler or query-IR descriptor uses
   provider layout `5` and the one existing V5 schema hash
   `EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V5`. A query-semantic type whose
   version is not a provider-state identity may retain its own accepted
   versioned name, but it must not mint, alias, or select a predecessor
   provider-state identity. A predecessor non-null order lowers each term to
   `PresentOnlyV1` only when the compiler proves the complete normalized
   predicate and installed lineage admit no missing or null value; nullable
   families retain their accepted `NullsFirstV1` or `NullsLastV1` placement.

3. V5 remains byte- and semantics-exact to accepted ADR-0145 and merged main:
   checkpoint framing and codec, canonical row and field order, comparison
   profiles, one `NoValue` class, direction-independent null placement, exact
   count, direct ordinal selection, policy scope, epoch/readiness, retention,
   bounds, atomic mutation, compaction, recovery, and typed failures do not
   change. This is a `breaking_epoch` retirement, not a new V5 format.

4. WP-757 deletes V1 through V4 provider-state constants, descriptor
   selections, readers, writers, structs whose identity is the retired format,
   fixtures, source locators, topology rows, compatibility aliases, tests that
   claim current readability, and runtime dispatch. Numeric identities,
   schema hashes, magic values, and symbolic identities remain reserved and
   may not name a new encoding. `ExactTextPartitionIndexV2`,
   `ExactTextPartitionIndexV3`, and every other predecessor provider-state
   implementation are deleted rather than hidden behind a V5 descriptor or
   compatibility wrapper. `ExactPredicateProgramV1` remains only as the nested
   semantic base required by canonical `ExactPredicateProgramV2` and V5 bytes
   and decoding; it is never a standalone provider-state selector or runtime
   provider. Every exact family executes through genuine current V5 state and
   semantics.

5. Every owning checkpoint boundary rejects V1 through V4 bytes from their
   framing identity before payload decoding, reinterpretation, mutation, or
   publication. No generic decoder, fallback, negotiation, hidden alias,
   migration mode, or caller-selected compatibility path survives.

6. Existing authoritative entity state and commit history are unchanged.
   Epoch two creates V5 only by the accepted bounded derived-state rebuild,
   catch-up, validation, and publication path. It never rewrites, relabels,
   translates, or adopts V1 through V4 checkpoints in place; requests retain
   their typed unavailable/refusal behavior until V5 is servable.

7. This record makes three narrow compatibility amendments after acceptance:

   - ADR-0131 Compatibility and topology language no longer retains its initial
     provider-state reader or least-sufficient writer in epoch two; its exact
     text truth profile and application behavior remain unchanged under V5.
   - ADR-0134 Decision 7 and Compatibility no longer retain or emit provider
     state V1 through V4 in epoch two; the complete predicate/order algebra,
     query semantics, and deployment readiness rules remain exact under V5.
   - ADR-0145 Decision 4 and Compatibility replace V4 readability and the
     least-sufficient V4 writer with sole-current V5; V5 bytes and the complete
     nullable total-order semantics remain exact.

8. ADR-0216 and its exact retirement of ADR-0185 OBL-0185-1 must be accepted
   and reconciled before WP-757 rotates query plans, modules, locks, cursors,
   generated fixtures, or hashes for this decision. The protected predecessor
   plan hash on main `0888d36326fc06fa32229cf5b6a684fe87a033ae` is
   `b79416ede1e8256cb2d2accabdaedc5afd1a87161fbf073f9249867aff983b1e`.
   Clean integration-safe tip `067a4ed61126857e1d052cefea155e85d92817b9`
   already produces the pre-provider intermediate
   `e204131728f1714f43e575cc3c8a28eec7b0c320d1a7841d32acb7a4ce7fbf3e`.
   This record assigns no cause to that existing difference and does not treat
   it as a V5 or final successor. After both decisions are reconciled,
   implementation independently derives and reviews every final combined
   plan/module/fixture successor and stops on any unexplained difference rather
   than editing an accepted hash assertion to pass.

9. This record narrowly amends ADR-0216 Decision 11: its prohibition on plan,
   provider-descriptor, and rebuildable provider-format changes has exactly the
   sole-V5 descriptor/state retirement and directly caused reviewed plan,
   module, lock, cursor-binding, generated-fixture, and hash rotations in
   Decisions 1 through 8 as an exception. ADR-0216 still solely governs
   V14/V18/V18 and generated-operation identities; every query and provider
   semantic remains exact. Production edits are limited to
   `crates/riffdb-types/src/exact_text.rs`,
   `crates/riffdb-query-compiler/src/lib.rs`,
   `crates/riffdb-query-ir/src/exact_predicate.rs`,
   `crates/riffdb-query-module/src/exact_text_result_set.rs`,
   `crates/riffdb-projection/src/exact_text.rs`,
   `crates/riffdb-projection/src/exact_predicate.rs`,
   `crates/riffdb-query-executor/src/exact_result_set.rs`, and
   `crates/riffdb-server/src/exact_text_adapter.rs`. They classify only surface
   or internal, so this record authorizes no new guarantee-classified path;
   ADR-0204 Decision 2 and all storage behavior remain unchanged.

10. After exact acceptance, a prerequisite WP-757-only path-authority commit
    touches only `work_packages.yaml` and adds exactly the ADR-0131, ADR-0134,
    and ADR-0145 paths to `WP-757.allowed_paths`, solely because the following
    reconciliation must edit the three legacy records under Decision 7. A
    following, separate WP-757-only governance commit updates those records,
    adds ADR-0219 to `required_adrs`, adds this record's requirements and
    OBL-0219-1, and reconciles the package objective, deliverables, exit gate,
    checks, and triggers to Decisions 1 through 9. ADR-0219 itself is not
    edited and needs no added package path. Neither commit contains runtime,
    fixture, generated-artifact, or implementation changes, and no path
    widening is inferred from this proposal before acceptance.

11. No query grammar, predicate, provider algorithm, comparison, order/null
    behavior, result, count, offset, policy, freshness, cursor, public surface,
    authority, protocol, authoritative storage format, or transaction behavior
    changes. ADR-0216 alone governs query-language, query-IR, query-module, and
    generated-operation identity retirement; this record governs only the
    exact-provider state identity and directly caused artifact rotations.

## Options considered

1. Retaining V1 through V4 as hidden aliases or read-only formats is rejected
   because it preserves the exact carrying cost WP-757 must delete.
2. Reinterpreting old checkpoints as V5 is rejected because it would make V5
   compatibility false and bypass the rebuild/readiness proof.
3. Updating the ADR-0185 hash fixture alone is rejected because it would hide
   a real plan-identity rotation before ADR-0216 retires its owning obligation.
4. Sole-current V5 after the coordinated query transition is chosen because it
   preserves exact semantics while satisfying the epoch-two topology.

## Consequences

- Epoch two has one exact-provider checkpoint identity and no predecessor
  decoder or writer surface.
- Existing databases cross only through export/reimport and rebuild V5 from
  authoritative state; no old checkpoint is an import artifact.
- Query plan, module, lock, cursor, generated, and hash evidence affected by
  the combined accepted transitions must rotate together under explicit review;
  the existing main-to-integration difference carries no causal claim here.
- Acceptance of ADR-0216 and this record, followed by two isolated governance
  commits, is required before implementation resumes.

## Standing design tests

- **Interface safety:** applications, agents, transports, and configuration
  cannot select a provider-state identity, old decoder, fallback, rebuild
  shortcut, or weaker query guarantee. Existing finite generated operations
  and typed lifecycle outcomes remain unchanged.
- **Scale:** V5 rebuild, catch-up, state amplification, index work, rank/select,
  pages, output, waits, and diagnostics retain their accepted bounds. The
  decision adds no full-state request path, matching-population materialization,
  co-location assumption, or in-place database rewrite.

## Checks

- `epoch_two_exact_provider_state_is_sole_v5_and_plan_rotations_are_exact`
  scans all owning source and artifacts for V1 through V4, proves V5 is the
  sole descriptor/codec/topology identity, refuses mutated and frozen old
  bytes before interpretation, and derives the reviewed plan/hash rotations.
- Exact-provider reference, nullable-order, count/ordinal, policy, rebuild,
  compaction, recovery, topology, inventory, generated-artifact, requirement,
  allowed-path, workspace, clippy, and `ci-all` checks preserve every unchanged
  semantic and compatibility guarantee.
