---
adr: "0302"
title: Retire Stranded Epoch-Two Reset Obligations
status: accepted
tier: guarantee
date: 2026-10-14
accepted: '2026-10-14'
acceptance: 'maintainer, in session, 2026-10-14: "i accept"'
requires: [ADR-0181, ADR-0190, ADR-0192, ADR-0198, ADR-0204, ADR-0207, ADR-0209, ADR-0211, ADR-0215, ADR-0216, ADR-0217, ADR-0219, ADR-0223, ADR-0224, ADR-0258, ADR-0263, ADR-0266, ADR-0276]
amends: [ADR-0181, ADR-0190, ADR-0192, ADR-0204, ADR-0207, ADR-0209, ADR-0215, ADR-0216, ADR-0219, ADR-0223, ADR-0224]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, GOV-004, AFC-001, AFC-002, AFC-005, AFC-006, VER-001, VER-002, VER-003, VER-004, VER-005, VER-008]
packages: [WP-757, WP-779]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - Any retired obligation would be relabelled as a completed proof rather than retired whole.
  - A retired obligation's exact tuple (ID, package, proof, says) would not remain visible in this record after retirement.
  - Any surviving obligation of WP-757 (OBL-0217-1, OBL-0217-2, OBL-0217-3) or WP-779 (OBL-0198-1, OBL-0198-2, OBL-0198-3, OBL-0211-1, OBL-0211-2) would be weakened, retired, or reinterpreted by this record.
---
# ADR-0302: Retire Stranded Epoch-Two Reset Obligations

## Context

Eight accepted records — ADR-0181, ADR-0192, ADR-0204, ADR-0207, ADR-0209,
ADR-0216, ADR-0219, and ADR-0224 — bind WP-757 to seventeen obligations and
WP-779 to one follow-on obligation (`OBL-0216-3`) for a destructive pre-alpha
Epoch-2 format reset (`epoch-2/writer-1`). Those eighteen obligations were
built on the `codex/wp757-integration-safe` prototype branch against `main` at
`886c50232465631cb5e7ffffe700fdc0b15d29c9` (`2026-09-05`), which never merged
and shares no post-filter-repo ancestry with `main`.

Subsequent accepted architecture on `main` took the non-destructive path
instead:

- ADR-0263 Amendment 5 and ADR-0276 Decision 1 (`WP-814`, `WP-815`, `WP-825`)
  explicitly keep Alpha Format Epoch 1 (`epoch-1/writer-0` through `writer-4`),
  retain predecessor offline writer-upgrade edges and frozen corpora, and
  reject a destructive epoch reset.
- ADR-0231 (`WP-760`) and ADR-0266 Decision 1 (`WP-824`) preserve the physical
  `exact_text`, `exact_predicate`, `nullable_predicate`, `tokenized_text`, and
  `long_pattern` provider implementations under a shared 1-GiB lifecycle budget.
- `WP-807` (`d008fc25f`) preserves multi-era contract fixtures (`V12`, `V15`,
  `V20`, `V21`) alongside `V24` through `V26`.
- `WP-779` (`cb0e3b0bc`) landed `riffdb-generated-application-operations/v6`
  and proved `OBL-0198-1`, `OBL-0198-2`, `OBL-0198-3`, `OBL-0211-1`, and
  `OBL-0211-2` while preserving readable `v2` through `v5` profiles.
- `WP-757` (`24c3742e0`) landed ADR-0217's snapshot-bound export of
  segment-owned durable events, provenance, and bounded sparse pages, proving
  `OBL-0217-1`, `OBL-0217-2`, and `OBL-0217-3` on `main`.

Executing the destructive Epoch-2 deletion now would violate ADR-0204's
`886c50232` closure trigger, ADR-0216's pre-`WP-779` ordering trigger,
ADR-0266's provider-preservation decision, and ADR-0276's Epoch-1 writer-edge
decision. Just as ADR-0258 retired the thirteen stranded `WP-705` obligations
whose unmerged branch machinery was superseded on `main`, these eighteen
stranded Epoch-2 reset obligations block `WP-757` and `WP-779` closure.

## Decision

1. Retire the eighteen stranded Epoch-2 reset obligations whole, on the exact
   ADR-0216 Decision 9 and ADR-0258 Decision 1 pattern: each exact unproved
   tuple remains visible below, none is relabelled as a completed proof, and
   each leaves the live `obligations` array of its owning record because `main`
   adopted in-epoch writer transitions (ADR-0263, ADR-0276) and preserved
   provider/era compatibility (ADR-0231, ADR-0266, `WP-807`) rather than the
   unmerged Epoch-2 reset branch.
2. After exact acceptance, one governance commit removes these eighteen entries
   from the live `obligations` arrays of ADR-0181, ADR-0192, ADR-0204,
   ADR-0207, ADR-0209, ADR-0216, ADR-0219, and ADR-0224; updates the
   corresponding `obligation_sources` in ADR-0190, ADR-0207, ADR-0209,
   ADR-0215, ADR-0216, ADR-0219, and ADR-0223 to `struck_obligations` (while
   retaining live `OBL-0207-2` in ADR-0207); and adds this record to
   `WP-757` and `WP-779` `required_adrs` and `allowed_paths`.
3. Every surviving obligation of those records and packages remains live and
   proven on `main`: `OBL-0181-4`, `OBL-0181-5`, `OBL-0181-6`, `OBL-0192-1`,
   `OBL-0192-2`, `OBL-0192-5` through `OBL-0192-9`, `OBL-0207-2`,
   `OBL-0217-1`, `OBL-0217-2`, `OBL-0217-3`, `OBL-0198-1` through
   `OBL-0198-3`, and `OBL-0211-1` through `OBL-0211-2`.
4. Any future breaking format-epoch reset requires its own decision record,
   contemporary baseline commit, and fresh obligations.

### Retired whole, with exact tuples preserved

- `OBL-0181-1` (ADR-0181, WP-757) proof `topology_refuses_second_readable_identity_before_external_database`; says, "Every topology domain carries exactly one readable and one writable identity, and the topology check refuses a second readable identity while no external database is registered."
- `OBL-0181-2` (ADR-0181, WP-757) proof `epoch_two_ceremony_refuses_in_place_open_and_reconciles_reimport`; says, "An epoch-1 database is refused in place by an epoch-2 binary and crosses only through the complete ADR-0112 ceremony with reconciled export/reimport counts and hashes."
- `OBL-0181-3` (ADR-0181, WP-757) proof `epoch_two_fixture_inventory_contains_no_epoch_one_identity`; says, "The durable-fixture inventory and release manifest name no epoch-1 identity after the reset, and every deleted Protobuf field number and enum value remains reserved."
- `OBL-0192-10` (ADR-0192, WP-757) proof `epoch_two_removes_legacy_vector_control_bridge`; says, "Epoch 2 removes tag-65 source, decoder, table, fixture, manifest entry, and topology exception while permanently reserving every retired numeric identity."
- `OBL-0204-1` (ADR-0204, WP-757) proof `epoch_two_format_gate_precedes_retired_reader_dispatch`; says, "The immutable epoch comparison refuses every epoch-1 database before any retired storage reader, table open, journal replay, repair, allocator action, or mutation can run, while missing, malformed, unknown, and future identities remain fail-closed."
- `OBL-0204-2` (ADR-0204, WP-757) proof `epoch_two_removes_legacy_vector_control_bridge`; says, "Epoch 2 removes tag-65 vector control without translating its bytes, preserves exact tag-67 control and V2 semantics, and retains cold registration and automatic first-demand activation."
- `OBL-0204-3` (ADR-0204, WP-757) proof `epoch_two_descriptor_closure_rotations_are_exact`; says, "The exact declaration and identity retirements produce only the 27 enumerated retained-record descriptor-closure hash rotations; retained payload bytes, fields, tags, revisions, and semantics stay exact, and predecessor hashes remain reserved and refused."
- `OBL-0207-1` (ADR-0207, WP-757) proof `topology_allows_only_exact_changelog_compatibility_exception`; says, "While the pre-external window is open, the topology accepts exactly the frozen authoritative-changelog V1/V2/V3 reader set with V3 as its sole writer and current identity, and continues to reject every other multiple-identity domain or variation of that set."
- `OBL-0209-1` (ADR-0209, WP-757) proof `epoch_two_columnar_control_lifecycle_is_v2_only`; says, "Epoch 2 accepts exactly the closed V2 control shapes and refuses every V1, CatchingUp, missing-fingerprint, mixed-layout, malformed, or unenumerated semantic shape."
- `OBL-0209-2` (ADR-0209, WP-757) proof `epoch_two_columnar_reset_allocates_transaction_current_v2_candidate`; says, "Fresh insertion, stale-incarnation reset, initial retarget, and failed-initial replacement allocate only the exact checked V2 candidate with deterministic physical fingerprint and reread-only uncertainty resolution."
- `OBL-0209-3` (ADR-0209, WP-757) proof `epoch_two_first_demand_publishes_v2_without_v1_artifact_or_dispatch`; says, "Cold startup opens no artifact and first semantic demand builds, validates, publishes, and installs only V2 without any V1 codec, artifact, prepared witness, transition, or fallback."
- `OBL-0209-4` (ADR-0209, WP-757) proof `epoch_two_unprepared_v2_retires_exact_candidate_paths`; says, "Every unprepared V2 candidate construction or restart repeats the exact aggregate-bounded final, temporary, quarantine, rename, sync, reference, and crash protocol before construction."
- `OBL-0209-5` (ADR-0209, WP-757) proof `epoch_two_prepared_initial_v2_head_advance_allocates_checked_replacement`; says, "A prepared initial V2 candidate behind transaction-current head is retired only through the exact checked-next unprepared initial V2 replacement CAS and every refusal or uncertain result resolves by durable reread."
- `OBL-0216-1` (ADR-0216, WP-757) proof `epoch_two_query_identity_topology_is_sole_current`; says, "Epoch two admits only RiffQL V14, query IR V18, and query module V18, refuses every retired reader and artifact, and preserves operator semantics without a version fallback."
- `OBL-0216-2` (ADR-0216, WP-757) proof `epoch_two_generated_application_operations_v5_is_sole_current`; says, "Epoch two admits only generated application operations V5, deletes V2 through V4 readers, writers, and artifacts, and preserves pagination, SDK-only, and vector semantics within V5."
- `OBL-0216-3` (ADR-0216, WP-779) proof `generated_application_operations_v6_is_sole_current_for_all_registries`; says, "While the external-database registry is empty, V6 succeeds sole-current V5 for empty and nonempty command registries and refuses every retired artifact without changing descriptor semantics."
- `OBL-0219-1` (ADR-0219, WP-757) proof `epoch_two_exact_provider_state_is_sole_v5_and_plan_rotations_are_exact`; says, "Epoch two admits only exact-provider state V5, removes and refuses every V1 through V4 source and artifact, and reviews the exact resulting plan, module, fixture, and hash rotations without changing exact-provider semantics or V5 bytes."
- `OBL-0224-1` (ADR-0224, WP-757) proof `epoch_two_folded_exact_inputs_are_sealed_and_v5_bytes_remain_exact`; says, "Compiler-sealed matched-form bindings preserve folded and binary truth, original output, policy and epoch isolation, and exact V5 encoding without predecessor providers or unbound normalization."

## Options considered

1. **Retire whole on the ADR-0258 pattern:** selected; preserves every exact
   unproved tuple truthfully without breaking ADR-0276's Epoch-1 writer edges,
   ADR-0266's provider registries, or `WP-825`'s active rehearsal campaign.
2. **Execute the destructive Epoch-2 deletion on current `main`:** rejected;
   contradicts ADR-0276 Decision 1, invalidates `OBL-0276-1` and `WP-807` era
   proofs, and violates ADR-0204's `886c50232` closure trigger.
3. **Rewrite the eighteen obligations to Epoch-1 checks:** rejected; re-pointing
   Epoch-2 deletion proofs at Epoch-1 compatibility checks would falsify what
   those obligations stated.

## Consequences

- `WP-757` owns its real `main` deliverables and proofs (`OBL-0217-1` through
  `OBL-0217-3`), and `WP-779` owns its real `main` proofs (`OBL-0198-1` through
  `OBL-0198-3`, `OBL-0211-1`, `OBL-0211-2`).
- Existing Epoch-1 writer-upgrade edges (`ADR-0276`), provider state registries
  (`ADR-0266`), and `WP-825` rehearsal artifacts remain intact.

## Standing design tests

- **Interface safety:** no public surface, format selector, fallback, or
  opt-out is added or changed. This record updates obligation bookkeeping only.
- **Scale:** no runtime path, bound, or storage layout changes.

## Checks

- `./scripts/check-adr-obligations` reports zero errors and, after the
  retirement commit, zero outstanding obligations across the repository.
- `./scripts/check-adr-form` and `./scripts/adr-index --check` pass.
- `OBL-0217-1` through `OBL-0217-3`, `OBL-0198-1` through `OBL-0198-3`, and
  `OBL-0211-1` through `OBL-0211-2` remain proven.
