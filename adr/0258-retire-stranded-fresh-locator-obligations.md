---
adr: "0258"
title: Retire Stranded Fresh-Locator Obligations
status: accepted
tier: guarantee
date: 2026-09-23
accepted: '2026-09-23'
acceptance: 'maintainer, in session, 2026-09-23: "i accept them" (ADR-0258 and ADR-0259)'
requires: [ADR-0205, ADR-0206, ADR-0208, ADR-0210, ADR-0213, ADR-0214, ADR-0236]
amends: [ADR-0205, ADR-0206, ADR-0208, ADR-0210, ADR-0213, ADR-0214]
supersedes: []
requirements: []
packages: [WP-705]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - Any retired obligation would be relabelled as a completed proof rather than retired whole.
  - A retired obligation's exact tuple (ID, package, proof, says) would not remain visible in
    this record after retirement.
  - OBL-0205-3, the one surviving obligation of the six records, would be weakened, retired,
    or reinterpreted by this record.
---
# ADR-0258: Retire Stranded Fresh-Locator Obligations

## Context

Six accepted records — ADR-0205, ADR-0206, ADR-0208, ADR-0210, ADR-0213 and
ADR-0214 — bind WP-705 to fourteen obligations. Thirteen of them name
fresh-locator coverage, arming, pre-arm preservation, the passive rebuild
observation of coverage, or the queued seal witness: the machinery ADR-0236
(accepted 2026-09-17, implemented by WP-787 in 98c198450) removed from main.
Their implementations were built on the `codex/wp-705*` branches, which never
merged — those branches have no common ancestor with main's re-bootstrapped
history — so no proof of them ever landed, and the machinery they would prove
no longer exists. `check-adr-obligations` reports all thirteen outstanding,
and WP-705 cannot close while they are undischarged.

ADR-0236 superseded ADR-0197 and ADR-0235 but did not touch the six WP-705
records, because its subject was the mechanism, not the obligations that
bound proofs to it. One obligation of the six records survives untouched:
OBL-0205-3 (request-scoped terminal-audit cause preservation), whose
machinery ADR-0236 did not remove; it is proven on main by
`request_scoped_command_terminal_audit_failure_preserves_threshold`
(ba71eb6cf) and is not affected by this record.

## Decision

1. Retire the thirteen obligations whole, on the ADR-0216 Decision 9 pattern:
   each exact unproved tuple remains visible below, none is relabelled as a
   completed proof, and the reason is the same for all — the machinery the
   proof names was removed by ADR-0236, and the implementations built for it
   never merged to main.
2. After exact acceptance, one WP-705-only governance commit removes exactly
   these thirteen entries from the live `obligations` arrays of their six
   records and adds this record to WP-705's `required_adrs`. No other field,
   decision text, review trigger, requirement, or record changes. The six
   records' decisions, and every surviving obligation in them, remain exact.
3. Nothing in this record weakens a check that still runs. ADR-0236 Decision 4
   already decided, one by one, which structural checks the queued witness
   performed survive independently; ADR-0236's own three proofs
   (`absence_answers_are_unchanged_without_fresh_locator_coverage`,
   `queued_seal_structural_checks_survive_coverage_removal`,
   `bounded_history_fallback_remains_the_only_absence_backstop`) own that
   surface. This record retires only the stranded proof obligations.
4. If a future record reintroduces a fresh-locator-shaped mechanism, it must
   carry its own obligations and proofs. Nothing here is reserved, promised,
   or half-kept.

### Retired whole, with exact tuples preserved

- `OBL-0205-1` (ADR-0205, WP-705) proof
  `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`;
  says, "A real production grouped coordinator executes the first fresh
  command through the live fused transaction path and every later new-key
  inspection uses exact public coverage with zero fallback history scans,
  without a test or caller manually arming it."
- `OBL-0205-2` (ADR-0205, WP-705) proof
  `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`;
  says, "Architecture checks prove the sole production arming call is in the
  shared fused command transaction constructor reached by both direct and
  deferred command batch paths before command staging."
- `OBL-0206-1` (ADR-0206, WP-705) proof
  `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`;
  says, "A verified bounded fast start keeps transient indexes Dormant while
  two real production Group-durability commands and their post-publication
  novel-key inspections complete with zero rebuilds and zero fallback history
  scans without manual arming."
- `OBL-0206-2` (ADR-0206, WP-705) proof
  `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`;
  says, "Architecture checks freeze the sole shared arming site and the
  private one-row exact logical-table emptiness implementation for both
  direct redb and deferred composite-stage access."
- `OBL-0208-1` (ADR-0208, WP-705) proof
  `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`;
  says, "An exact command-empty clean start prepares and executes two audited
  production Group commands through shared ports while retained operational
  ports prove zero transient rebuilds and both novel-key inspections prove
  zero fallback scans."
- `OBL-0208-2` (ADR-0208, WP-705) proof
  `exact_empty_command_authority_avoids_command_audit_index_activation`; says,
  "Architecture checks freeze the same-read-transaction exact-empty
  command-tail branch, complete physical administration validation, and
  unchanged derived-index validation for every nonempty history."
- `OBL-0210-1` (ADR-0210, WP-705) proof
  `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`;
  says, "A cloned passive controller proves zero transient-index rebuilds
  while two real production Group commands, their distinct post-publication
  novel-key inspections, and successful coordinator shutdown complete with
  zero fallback scans."
- `OBL-0210-2` (ADR-0210, WP-705) proof
  `passive_rebuild_observation_matches_one_real_rebuild`; says, "A calibration
  test causes exactly one real transient-index rebuild and proves the cloned
  controller reports one in parity with the operational-port counter."
- `OBL-0210-3` (ADR-0210, WP-705) proof
  `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`;
  says, "Architecture checks freeze the passive controller mirror, its sole
  increment site, both shared-state rebuild sites, the bounded-start
  exclusion on the raw ephemeral rebuild, saturation, and the fresh-locator
  arming location and bounds."
- `OBL-0213-1` (ADR-0213, WP-705) proof
  `fresh_locator_recognized_pre_arm_control_writes_preserve_uninitialized`;
  says, "Exact recognized pre-arm preserving Immediate writes retain
  Uninitialized without minting a witness, while every invalid, unknown,
  uncertain, poisoned, or lost-witness case disables coverage and Disabled
  remains monotonic."
- `OBL-0213-2` (ADR-0213, WP-705) proof
  `fresh_locator_queued_frame_validator_is_linear_exact_and_bounded`; says,
  "The queued command witness validates every ADR-0197 Decision 6 fact from
  the exact mutations returned by the same composite seal, the sealed frame
  metadata, and ordered deltas in bounded one-pass work, decoding each
  physical segment once and refusing all missing, duplicate, extra,
  sequence-reordered, malformed, substituted, oversized, and uncertain
  evidence."
- `OBL-0213-3` (ADR-0213, WP-705) proof
  `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`;
  says, "The real bounded-fast-start Group path preserves Uninitialized
  across its recognized startup controls, arms on its first command, and
  completes two commands and distinct novel inspections with zero rebuilds
  and zero fallback scans."
- `OBL-0213-4` (ADR-0213, WP-705) proof
  `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`;
  says, "Architecture checks freeze pre-arm state handling and the queued
  validator's exact frame source, one-pass bounds, complete fact inventory,
  and prohibition on point, history, composite-successor, and repeated
  manifest reads."

## Options considered

1. **Retire whole on the ADR-0216 pattern.** Selected. It is this
   repository's established precedent (ADR-0216 Decision 9 retired
   OBL-0198-3 the same way), it keeps every tuple visible, and it does not
   pretend a proof ran.
2. **Amend each of the six records individually.** Rejected: six acceptance
   ceremonies for one reason, and six chances to drift.
3. **Rewrite the obligations to surviving machinery.** Rejected: re-pointing
   proofs at the bounded history fallback or the absence-order check would
   create new obligations against machinery those records never decided,
   repeating the drift this record exists to close.
4. **Transfer to a new owner package.** Rejected: it keeps the obligations
   live against nothing and blocks WP-705's closure on a package that does
   not exist.

## Consequences

- WP-705's obligation surface becomes exactly what is real: OBL-0205-3,
  proven, plus the package's requirement tags.
- The six records keep their decisions and review triggers; only the stranded
  proof bindings leave their live arrays, with the tuples preserved here.
- Explicitly deferred: any future fresh-locator-shaped mechanism requires its
  own record with its own obligations and proofs.

## Standing design tests

- **Interface safety:** nothing becomes selectable, observable, or bypassable
  that ADR-0236 did not already decide. This record changes proof
  bookkeeping, not behavior.
- **Scale:** no runtime surface changes; the retirement removes no check that
  runs.

## Checks

- `./scripts/check-adr-obligations` reports the thirteen IDs outstanding
  before the retirement commit and none after it, with OBL-0205-3 proven
  before and after.
- `./scripts/check-adr-form` and `./scripts/adr-index --check` pass on the six
  amended records and this one.
- `request_scoped_command_terminal_audit_failure_preserves_threshold` passes
  unchanged.
