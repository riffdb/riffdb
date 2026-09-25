# WP-760 startup-test adaptation — round 2 report (design settled)

Final state: `cargo test -p riffdb-storage-redb --all-features --lib startup::` is
**91 passed / 0 failed / 1 ignored** (the ignored test is
`shutdown_checkpoint_build_scale_evidence`, ignored by design as a scale
benchmark). Baseline at the start of this round was 73/18/1. All 18 reds were
re-baselined to the settled bounded-dirty design; no fail-closed assertion was
weakened without an explicit note below; every `// req:` tag is preserved
(verified by diff against HEAD — only line numbers shifted).
`cargo fmt --all` and `cargo clippy -p riffdb-storage-redb --all-targets
--all-features -- -D warnings` are clean. Nothing is committed.

Note: the orchestrator's `/tmp/wp760-adaptation-notes-v2.md` was never written to
disk (the heredoc command in the orchestrator transcript never executed). Its
full content was recovered from
`.cowboy/sessions/1790181776692-1/transcript.jsonl` line 3122 and saved to
`/tmp/wp760-adaptation-notes-v2.md` and `cowboy-work/wp760-adaptation-notes-v2.md`
before work began; the design facts below follow it exactly.

## The settled design (as applied)

The bounded dirty branch (`startup.rs`, `begin_structural_evidence_for`) engages
only when ALL of: purpose == Startup, v3_active, record registry CURRENT, an
active validated-prefix checkpoint covers the current state
(`load_active_checkpoint` Ok), and not follower mode. Consequences applied
throughout:

- FIRST open under the current registry: COMPLETE path, writes the checkpoint at
  finish → 3 receipts (activation + ValidatedPrefixCheckpoint + dirty), epoch +2.
- LATER dirty opens (checkpoint present, no clean cert): BOUNDED path → +1 dirty
  receipt, epoch +1, `session.bounded_dirty_startup_path()` true,
  `checkpoint_verified()` false, `checkpoint_ignored_reason()` None.
- Clean-certificate opens: unchanged clean bounded path.
- Follower opens: always complete (branch condition excludes them).
- Detection assertions that need the complete walk:
  `crate::startup::begin_background_scrub_on_shared(&shared, inputs())`.

## Per-test changes

### startup_v3_tests.rs (4 tests)

1. `actual_fresh_activation_crashes_leave_exact_inactive_or_complete_v3_and_retry_once`
   — edge "committed" leaves V3 active under the current registry but with NO
   checkpoint (the crash preceded any complete walk), so the retry STILL takes
   the complete path and writes the checkpoint: 3 receipts in every arm
   (was `if committed {2} else {3}`). Comment updated.
2. `actual_v3_bounded_dirty_and_clean_consumption_keep_exact_receipts` —
   restructured to prove all three paths in order: first open = COMPLETE
   (3 receipts [activation, prefix-checkpoint, dirty], epoch +2,
   `!bounded_dirty_startup_path()`); then an explicit LATER dirty reopen =
   BOUNDED (`bounded_dirty_startup_path()` true, +1 dirty receipt → 4 total,
   epoch 1 for that process, prefix bytes retained); then the clean-close loop
   re-baselined to [6, 8] (was [4, 6] under the round-1 assumption that the
   first open was bounded).
3. `actual_v3_startup_and_scrub_refuse_missing_or_corrupt_interior_history_before_writes`
   — first open is complete → receipt set is [activation, prefix-checkpoint,
   dirty] (3, was 2). Arm 1 now corrupts the interior prefix-checkpoint row
   (seq 2); arm 2 corrupts the tail (seq 3, was 2). The refusal routing
   (engine-open reconciliation or the scrub's retained-history validation,
   stable across repeated opens, no writes) is unchanged.
4. `actual_v3_startup_process_crashes_recover_complete_prefix_and_lifecycle_receipts`
   — lifecycle edges run on the complete FIRST open (checkpoint receipt commits
   before DIRTY): expectations shifted +1 to [dirty-staged 2, dirty-committed 3,
   clean-staged 3, clean-committed 4] (was [1,2,2,3]). The direct-edge section
   (checkpoint receipt write during activation) was already correct and is
   unchanged.

### startup_v3_retention_tests.rs (3 tests)

Reverted the round-1 −1 shifts (round 1 assumed the first open was bounded;
under the settled design `initialized()`'s `open_cleanly` is the store's FIRST
open under the current registry → complete → 3 receipts and a checkpoint the
prune then invalidates):

5. `actual_offline_prune_receipts_preserve_exact_deletions_and_watermark_atomicity`
   — original 3-receipt baseline, prune adds invalidation + prune receipts
   (original counts 3/5/6), and the post-prune open is COMPLETE (checkpoint
   deleted by the prune) so `!ports.clean_close_fast_startup()` holds again.
   Comments updated to name the complete-first-open and post-prune-complete
   semantics.
6. `actual_v3_prune_preserves_per_transaction_refusal_unknown_and_retry_semantics`
   — original expectations (3/4/5, watermark 2 only at 5, final 5).
7. `actual_v3_prune_crashes_keep_checkpoint_invalidation_and_subrange_atomic`
   — original expectations ([3,4,4,5], final 5).

### startup_v2_primary_tests.rs (1 test)

8. `v2_clean_close_requires_complete_startup_and_keeps_lifecycle_bytes` — the
   reopen's clean certificate still declines (`bounded_roots_unavailable`,
   session-level assertions unchanged: CLEAN is insufficient for V2 authority),
   but the declined cert now routes to the BOUNDED DIRTY path (checkpoint from
   the first complete open, bounded roots re-validated directly). Added
   `session.bounded_dirty_startup_path()`; replaced the stale ports-level
   `!clean_close_fast_startup()` with `!ports.clean_certificate_startup()` —
   the precise "no clean CERTIFICATE was trusted" claim.

### startup/tests.rs (9 tests)

9. `the_shutdown_checkpoint_write_iterates_no_history_rows` — the reopen's
   `checkpoint_verified()` replaced by `bounded_dirty_startup_path()`: the
   bounded admission runs `load_active_checkpoint`, which verifies every
   recorded count against redb's own row counts, so the bounded session IS the
   count proof. Exact-ends finish retained.
10. `startup_republishes_once_and_shutdown_reuses_that_process_proof` — the
    second open is bounded and must NOT republish: `assert_ne` → `assert_eq`
    (byte-for-byte retention), and the shutdown write is now REFUSED by the
    compatibility writer's process-generation gate
    (`startup_validation_clean` is false on a bounded process) → `Ok(false)`,
    with the published bytes retained untouched. See "semantics moved" below.
11. `an_uncertain_current_checkpoint_takes_the_full_repair_path` — the repaired
    proof is validated by the bounded admission itself (self-hash, counts,
    entity-head snapshot): `checkpoint_verified()` →
    `bounded_dirty_startup_path()`. Full-validation finish retained.
12. `missing_checkpoint_head_snapshot_row_falls_back_and_repairs` — first reopen
    (damaged snapshot) unchanged: complete path, ignored reason
    `entity_chain_mismatch`. Second reopen after repair:
    `checkpoint_verified()` → `bounded_dirty_startup_path()` (the repaired
    entity-head snapshot verifies at load).
13. `a_committed_terminal_execution_failure_advances_the_census_it_is_counted_by`
    — `session_execution_failure_census` restructured: the plain reopen is
    bounded (admission accepts the maintained-census checkpoint; no walk, no
    seed); the checkpoint is then invalidated exactly as the ADR-0085 prune
    does, and the following COMPLETE open re-seeds the census (asserted == 1).
    The write-time census-maintenance assertions are unchanged.
14. `a_drifted_terminal_census_is_refused_at_the_next_open` — see "gap" below.
    Re-baselined to the design's actual fail-closed boundary: the drifted
    (under-reporting) checkpoint's binding ADMITS the bounded path (counts are
    not impossible; the exact census is not O(1)-derivable at load); the
    complete walk (forced by prune-style invalidation) classifies every row,
    re-seeds the TRUE census (0), and writes a corrected checkpoint that then
    admits. Arm 1 (impossible census → write-time fallback to the reference
    walk) unchanged.
15. `the_publication_proof_survives_the_validated_prefix_fast_path` — the
    checkpointed reopen is bounded (no population walk): asserts bounded
    admission, zero findings on the bounded walk, and ports still released
    (readiness not withheld for a derived-only defect). The orphan
    MissingCrossLink finding is then asserted from the background scrub's
    COMPLETE walk (`begin_background_scrub_on_shared`), which must refuse to
    finish over it. req: REP-003, REC-001 tag kept.
16. `projection_frontier_and_marker_prefix_defects_are_derived_only` — same
    layering: the bounded open must not enumerate population tables (zero
    findings) and must not withhold ports; the ProjectionStateMismatch findings
    are asserted from the background scrub's complete walk, which must refuse to
    finish over them. req: REP-003, REC-001 tag kept.
17. `sealed_offline_scrub_refuses_exclusive_owner_and_corruption_without_mutation`
    — the corruption inserts an UNKNOWN META key (`storage_format_version`,
    value `[0xff]`). The typed refusal kind moved from CorruptData to
    IncompatibleFormat with the (already-landed) ADR-0182 compact-encoding
    migration guard: the pre-current-registry META scan that transcode-failed
    this value no longer runs for current registries, so the refusing layer now
    classifies the unrecognized metadata inventory as IncompatibleFormat. The
    test now accepts either typed kind and still pins "refused, never repaired"
    plus the unchanged-authority snapshot assertions (which pass — no mutation).
    Exclusive-owner refusal arm unchanged.

### startup_graceful_close_tests.rs (1 test)

18. `graceful_checkpoint_close_crash_matrix_preserves_dirty_fallback` — the
    before-commit-arm dirty reopen is the bounded dirty path: the
    `checkpoint_verified()` assertion replaced by
    `session.bounded_dirty_startup_path()` (the failed graceful close left no
    clean certificate; the bounded admission itself proves the checkpoint
    verified at load). Exact-ends finish retained.

## Remaining reds

None in `startup::` (91/0/1). Two PRE-EXISTING failures elsewhere in the crate
(verified failing at HEAD `67d374673` in a clean worktree, untouched by this
round — no production code was changed here):

- `administration::tests::fenced_runtime::fenced_v2_clean_reopen_validates_retained_admission_without_repair`
  (primary_fenced_runtime_tests.rs:357) — asserts
  `!ports.clean_close_fast_startup()` on a reopen that now takes the bounded
  dirty path; same `clean_close_fast_startup` conflation as the v2_primary
  test (fixed here by using `clean_certificate_startup`).
- `store::tests::mixed_v1_v2_framing_resumes_and_publishes_registry_last`
  (store/tests.rs:1033) — registry-publication expectation under the settled
  design.

Both are round-3 candidates (same adaptation pattern as #8), out of this
round's startup:: scope.

## Gaps and semantics moved (for orchestrator review)

1. **Drifted-census refusal no longer exists at open (ACCEPTED-THEN-HEALED, not
   refused).** The pre-ADR-0182 open consumed the checkpoint's counts to
   compute a suffix walk and refused when the recorded prefix count disagreed
   with redb's row counts. Under the settled design no SOURCE path consumes
   those counts (the bounded path skips the population entirely; the scrub
   re-derives everything and does not load the checkpoint), and the exact
   census is not O(1)-derivable at load (the StoredOutcome/ExecutionFailed
   split is not observable from table lengths), so an under-reporting drifted
   checkpoint is admitted by the bounded branch. Mitigations that remain
   fail-closed: impossible counts are refused at admission
   (`CountImpossible`); the graceful-close classifier never retains a drifted
   proof as exact-current (`base.counts() == counts` compares against the
   process's own re-derived counts, and a bounded process's witness is false
   anyway); the next complete walk (first open, post-prune, follower) re-seeds
   the true census and rewrites a correct proof. Test #14 now documents this
   boundary explicitly. If the orchestrator wants the old REFUSAL semantics
   back, the production repair would be an exactness check at S==head in
   `load_active_checkpoint` for the count classes that ARE O(1)-derivable
   (commits/events/outbox/…; idempotency remains non-derivable) — a partial
   restoration only. NOT landed here (production change; design is settled).
2. **`ports.clean_close_fast_startup()` naming/semantics conflation.** The flag
   now means "reached readiness through ANY bounded path" (clean certificate OR
   ADR-0182 bounded dirty). Two tests (v2_primary here, fenced_runtime
   pre-existing red) read it as "clean-certificate fast path". The accurate
   per-mode flag `clean_certificate_startup()` exists; consider renaming or
   doc-hiding `clean_close_fast_startup()` in a follow-up. Also visible in the
   retention test where a bounded dirty open reports it true.
3. **Republish-per-process-generation semantics moved.** A bounded dirty open
   never publishes a process proof (compatibility writer gated on the process's
   own complete validation — correct fail-closed behavior, now pinned by test
   #10) and never re-seeds the terminal-execution-failure census (pinned by
   #13/#14). Both semantics now live only on complete walks: first open under
   the current registry, post-prune reopens, follower opens, and the scrubs
   (which write nothing). Production shutdown (`complete_graceful_close`) is
   unaffected: its classifier requires the process witness and leaves the
   previous proof untouched (LeftStale) on a bounded process.
4. **Unknown-META-key refusal kind moved** (test #17): CorruptData →
   IncompatibleFormat for current-registry stores, because the transcode scan
   that produced CorruptData is (correctly, per the landed guard) no longer run
   for current registries. Fail-closed behavior (typed refusal, zero mutation)
   is preserved and asserted; only the kind moved. If CorruptData is the
   contractually required kind for unknown META keys, the refusing layer should
   be re-classified in production — flagged for review, not changed here.
5. **Follower complete opens still consume checkpoint counts** (the only
   remaining count-consuming walk). A drifted-count checkpoint on a follower
   would hit the old refusal semantics there. Not covered by a test in this
   round (the drifted fixture is a source store); noted for completeness.

## Production repairs

**None landed.** This round changed test files only (plus a `cargo fmt`
line-reflow in `startup.rs` — no semantic change). The two candidate production
repairs considered were deliberately NOT landed and are documented above for
orchestrator decision: (a) exact count checks at S==head in
`load_active_checkpoint` (partial restoration of the drifted-census refusal),
and (b) re-classifying the unknown-META-key refusal kind. The follower-mode
exclusion in the bounded branch condition (from round 1) is retained unchanged.

## Validation

- `cargo test -p riffdb-storage-redb --all-features --lib startup::` →
  91 passed, 0 failed, 1 ignored.
- Full crate lib run: 728 passed, 2 failed (both pre-existing at HEAD, outside
  startup::, documented above), 1 ignored.
- `cargo fmt --all` clean; `cargo clippy -p riffdb-storage-redb --all-targets
  --all-features -- -D warnings` clean.
- All `// req:` tags preserved (diff-verified against HEAD).
- Nothing committed (working tree only), per instructions.
