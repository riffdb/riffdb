# WP-760 startup-test adaptation — round 2 (design settled)

## The settled design (do not re-litigate)
The bounded dirty branch (startup.rs, begin_structural_evidence_for) engages only when ALL
of: purpose==Startup, v3_active, record registry CURRENT, an active validated-prefix
checkpoint covers the current state (load_active_checkpoint Ok), and not follower mode.
Consequences:
- A database's FIRST open under the current registry takes the COMPLETE path (no checkpoint
  yet) and WRITES the checkpoint at finish (3 receipts: activation + prefix-checkpoint +
  dirty; epoch +2).
- Every LATER dirty open (checkpoint present, no clean certificate) takes the BOUNDED path
  (2 receipts: prior + dirty; epoch +1; no new checkpoint).
- Clean-certificate opens remain the clean bounded path (unchanged).
- Follower opens always take the complete path (the worker's earlier repair:
  !self.shared.is_follower_mode() in the branch condition — keep it).
- The open-time compact-encoding migration is guarded to pre-current registries (landed).
- The background scrub session (crate::startup::begin_background_scrub_on_shared) runs the
  complete walk for detection assertions.

## Current state
73 passed / 18 failed / 1 ignored in `cargo test -p riffdb-storage-redb --all-features
--lib startup::`. The 6 v3_migration tests now PASS. Remaining 18:

1. v3 exemplar tests (3) — MY earlier adaptations assumed the FIRST open is bounded; under
   the settled design the first open is complete (writes the checkpoint) and only LATER
   dirty opens are bounded. Re-baseline: first open = complete-path expectations (3
   receipts, epoch+2, checkpoint written); then a second dirty open = bounded expectations
   (bounded_dirty_startup_path() true, 2 receipts, epoch+1); the clean-close loop after.
   Tests: actual_v3_bounded_dirty_and_clean_consumption_keep_exact_receipts,
   actual_v3_startup_and_scrub_refuse_missing_or_corrupt_interior_history_before_writes
   (its arms corrupt history AFTER a complete open — the checkpoint exists; a corrupt
   history row may invalidate the checkpoint => the reopen may take the complete path and
   refuse there — assert whichever layer refuses, per the original adaptation),
   actual_v3_startup_process_crashes_recover_complete_prefix_and_lifecycle_receipts,
   actual_fresh_activation_crashes_leave_exact_inactive_or_complete_v3_and_retry_once.
2. census/checkpoint tests (7) in startup/tests.rs — their fixtures complete walks that
   write checkpoints; the post-checkpoint reopen is now BOUNDED (no checkpoint load, no
   census seed). Re-baseline to the bounded semantics OR drive a complete session where
   the test asserts complete-walk properties: a_committed_terminal_execution_failure...,
   a_drifted_terminal_census..., an_uncertain_current_checkpoint...,
   missing_checkpoint_head_snapshot_row..., the_shutdown_checkpoint_write...,
   the_publication_proof_survives..., startup_republishes_once...,
   projection_frontier_and_marker_prefix_defects...,
   graceful_checkpoint_close_crash_matrix...,
   sealed_offline_scrub_refuses_exclusive_owner_and_corruption_without_mutation.
3. v2_primary (1): v2_clean_close_requires_complete_startup_and_keeps_lifecycle_bytes —
   re-baseline receipt expectations per the design.
4. v3_retention (3): the prune tests; their checkpoint-invalidation assertions interact
   with the new preconditions — check whether the prune's checkpoint deletion now forces
   the complete path on reopen (probably CORRECT behavior: after a prune the checkpoint is
   deleted => the next open is complete => re-baseline).

## Rules (unchanged)
- Never weaken a fail-closed assertion; if no layer refuses a corruption shape, report it.
- Keep every // req: tag. Do not commit. fmt + clippy clean at the end.
- The artifact: /tmp/wp760-adaptation-report.md with per-test changes, remaining reds with
  root causes, and any production repairs marked for orchestrator review.