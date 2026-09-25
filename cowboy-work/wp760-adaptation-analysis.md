# WP-760 startup-test adaptation: root-cause analysis (in progress)

Status: analysis complete for all 27 failures; test edits not yet landed.
This file preserves the full diagnosis so the work can continue without re-derivation.

## Recovered orchestrator notes
The /tmp/wp760-adaptation-notes.md file was never written to disk; its full content was
recovered from the orchestrator session transcript
(/workspace/.cowboy/sessions/1790181776692-1/transcript.jsonl, line 2688) and matches the
task brief.

## Root causes (verified against code)

### A. V3-active source fixtures now take the bounded dirty path on reopen
`begin_structural_evidence_for` (startup.rs:928) takes the bounded branch when
`purpose == Startup && v3_active` and no verified clean-close certificate. Consequences
on that branch (startup.rs:917-1019, 1481-1596):
- Receipts on a dirty open: [V3Activation, DirtyActivation] (2, epoch +1); no
  ValidatedPrefixCheckpoint receipt.
- No checkpoint written, census not seeded, `startup_validation_clean` stays false.
- `saw_v1_index` never set (bounded historical plan =
  build_clean_close_historical_evidence_plan: bundles, contract-migration edges, active
  catalog ONLY — no SECONDARY_INDEXES/INDEX_EPOCHS rows), so `finish()` returns
  Clean, never MigrationRequired.
- `bounded_clean_startup` stored true at finish (line 1578-1581).

Affected tests and required adaptations:
- startup_v3_tests.rs `actual_fresh_activation_crashes...`: after.len() 3→2 (line 173).
- startup_v3_tests.rs `actual_v3_startup_process_crashes...`: expected receipt counts
  shift by −1 for every edge after the bounded change (old [1,1,1,2,2,3,3,4] →
  [1,1,1,2,2,3,3,4] minus the prefix-checkpoint receipt: direct edges unchanged at 1
  except committed 2→? — recompute: child writes activation(1) + dirty(2) + clean(3);
  crash edges: mutations/receipt/roots → 1; committed → 2; dirty-staged/committed → 2/3
  (dirty receipt is seq 2); clean-staged/committed → 3/4. Old expectations had +1 for the
  checkpoint receipt between dirty and clean; new: dirty-staged 2, dirty-committed 3,
  clean-staged 3, clean-committed 4 — wait, old was [.., 2,3,3,4] and failure showed
  left=101(right=93 means exit code mismatch: child exits 101 panic "requested startup/
  close crash edge was not reached" because clean-staged edge no longer fires? Actually
  101 vs 93: the child panicked (exit 101) instead of exiting 93 at the edge — the
  clean-staged edge never fired because... the write_clean_close_lifecycle path changed?
  No: failure at line 580 `assert_eq!(status.code(), Some(93))` got Some(101) for edge
  "clean-staged": the child reached the end without hitting the edge. Cause: the child's
  first open now takes the bounded path and the historical/structural walk is bounded, so
  the edge hook tied to a stage that no longer runs in that shape... Investigate: crash
  edges are in store_changelog_lifecycle.crash_edge (dirty-staged/dirty-committed) and
  presumably clean-staged/clean-committed in write_final_clean_close_lifecycle_after_
  barrier. The child does finish_v3 (bounded) then write_clean_close_lifecycle. If the
  clean lifecycle prepare returns None (no recovery roots?) the edge never fires. More
  likely: the edge fired but at a different point, or the bounded path consumed the
  dirty lifecycle so the CLEAN write happens... needs a quick empirical check: run the
  child manually with RIFFDB_V3_LIFECYCLE_EDGE=clean-staged and read its stderr.
- startup_v3_retention_tests.rs: `initialized()` uses open_cleanly on a V3-active store;
  receipts 3→2 at lines 43, 173, 219; per-arm expected counts in the two hook/crash
  tests shift by −1 (checkpoint-delete arm expectations 3/4/5 → 2/3/4 etc. — recompute
  against the 2-receipt baseline: original=2 receipts [activation, dirty]; prune adds
  invalidation+prune → 4; arms: checkpoint-staged 2, checkpoint-committed 3,
  subrange-staged 3, subrange-committed 4; final 4. Old [3,4,4,5]→new [2,3,3,4], final 5→4.
  Also `assert!(!ports.clean_close_fast_startup())` at line 140: bounded_dirty sets
  bounded_clean_startup=true so clean_close_fast_startup() is now TRUE after a bounded
  dirty open — the assertion's meaning changed; assert on the session instead
  (session.clean_close_fast_path() == false, session.bounded_dirty_startup_path() == true).
- startup_v2_primary_tests.rs `v2_clean_close_requires_complete_startup...`: line 243
  `assert!(!ports.clean_close_fast_startup())` now fails because... V2 source (initialize_
  database = SourceCreation::Current → V3-active!) — initialize_database stages a new
  source (current registry). So the reopen takes the bounded dirty path (no clean cert?
  it wrote CLEAN before drop... it did write_clean_close_lifecycle). Hmm: the session
  asserts !clean_close_fast_path with declined reason "bounded_roots_unavailable" — that
  passed (line 226-232 not the failure). Failure is line 243 ports-level flag. The reopen
  session declined the clean path (bounded_roots_unavailable — V2 catalog digest), so it
  fell to the bounded dirty branch? No — v3_active is true (current registry + roots),
  so it took the bounded dirty branch (line 928) which sets bounded_clean_startup=true at
  finish → ports.clean_close_fast_startup() true. The test's intent "CLEAN is
  insufficient for V2 authority" is still proven at the session level (line 225-232).
  Adaptation: keep session-level assertions; replace the ports-level assertion with
  session.bounded_dirty_startup_path() semantics or drop the ports flag check (the flag
  now means "bounded startup (clean OR dirty)", not "clean-certificate fast path").
  NOTE: this may also be a naming/semantics gap worth reporting: clean_close_fast_
  startup() conflates clean-fast and bounded-dirty.

### B. v3_migration tests (5): the Startup purpose never sees V1 index rows
`run_migration` uses begin_structural_evidence (Startup) → bounded branch → bounded
historical plan has no index rows → MigrationRequired never returned → panic "V1 rows
need migration" / wrong outcomes. Adaptation: drive the migration through the complete
path. Options: (a) open with the OfflineIntegrityScrub purpose — but that finish
(finish_offline_integrity_scrub) refuses saw_v1_index, it doesn't hand back a migration
port. (b) The migration port comes only from session.finish() when saw_v1_index. The
complete path is reached when NOT (purpose==Startup && v3_active) — i.e. these tests
need a way to open a complete Startup session on a V3-active store. Check: is there an
explicit complete-purpose API? EvidenceOpenPurpose has only Startup/OfflineIntegrityScrub/
BackgroundScrub. The offline scrub's validate_session REFUSES MigrationRequired
(IncompatibleFormat at line 250-252). So NO current surface drives the index migration
on a V3-active database except... check CatalogIndexMigrationDriver callers and
complete_partition_index_generation_migration (used by the two passing epoch-repair
tests). The 5 failing tests exercise CatalogIndexMigrationDriver via session.finish()
MigrationRequired. GAP CANDIDATE: the bounded dirty path removed the only Startup-shaped
route to the index-migration port; the offline scrub refuses it; the background scrub
refuses it (saw_v1_index check in finish_background_scrub). Unless the migration is
supposed to be driven by complete_partition_index_generation_migration or an explicit
complete startup remains reachable some other way, V1 index rows on a V3-active database
can no longer be migrated — that is a fail-closed REFUSAL (not corruption acceptance),
so the tests may need to assert the new refusal semantics OR the implementation needs a
complete-path escape hatch. Per ADR-0182 section 2, V1 rows are "validated on first
access" — but migration is a write operation. REPORT AS GAP/QUESTION for the
orchestrator: how should V1 index migration be driven now? Do not silently delete.

### C. Census/checkpoint tests in startup/tests.rs (7)
Fixtures use checkpointable_history_store + open_cleanly — but wait:
initialized_store = initialize_legacy_fixture = PRE_V3 registry, NOT V3-active. Yet
open_cleanly's finish() writes a checkpoint via the non-bounded path (v3_active false →
full path, line 1549-1572 writes checkpoint + sets startup_validation_clean(true)).
The reopen then... v3_active is false (pre-V3 registry, no V3 roots) → NOT the bounded
branch → full path → load_active_checkpoint should run. But failures show
checkpoint_verified()==false with ignored_reason None. ignored_reason None means the
Err(reason) arm at line 1128 did NOT run — i.e. active_checkpoint was
Err(CheckpointIgnoreReason::Absent)?? No — Absent also goes through that arm for
purpose==Startup. ignored_reason None + checkpoint_verified false happens when... the
Ok(active) arm ran but sample_finding_seen=true (line 1108-1116: drops checkpoint,
sets checkpoint=None, checkpoint_verified=false, and does NOT set checkpoint_ignored_
reason). So the sample windows found a finding! Why now? run_checkpoint_sample_windows
uses publication_audits built at line 1071 — unchanged. Hmm, OR: the checkpoint write
in open_cleanly's finish now FAILS (write_validated_prefix_checkpoint error → non-fatal,
note_checkpoint_write_failure) leaving META_VALIDATED_PREFIX_CHECKPOINT absent → reopen
gets Absent → but then ignored_reason would be Some("absent"). Failure prints None.
So sample_finding_seen path. What changed to make sample windows fail? Possibly nothing
in these tests' fixtures changed and the real cause is different: check
`the_shutdown_checkpoint_write` line 5214 `.expect("startup checkpoint")` — that PASSED
(checkpoint exists). Then reopen: checkpoint_verified false, reason None → sample
finding. Wait — actually re-check: maybe the checkpoint write at finish (line 1560) now
errors because... write_validated_prefix_checkpoint with v3=false writes fine. Hmm.
ALTERNATIVE: these fixtures' open_cleanly store IS V3-active? checkpointable_history_
store → initialized_store → legacy fixture → pre-V3. But open_cleanly finishes with
v3_activation_required=true (v3 inactive) → PendingV3Activation path (line 1519-1540) →
NO checkpoint write there! The checkpoint is written by pending.activate() inside
into_operational_after_catalog_validation (startup_v3_activation.rs:66) — which runs in
open_cleanly. That writes the checkpoint AND the DIRTY receipt. So after open_cleanly
the store IS V3-active with 2 receipts. The REOPEN then takes the bounded dirty branch
(v3_active, no clean cert) → checkpoint never loaded → checkpoint_verified false,
ignored_reason None (bounded branch returns early at line 971-1019 before the
checkpoint logic). THAT is the cause. ✔ Confirmed by exemplar pattern.

Adaptation for the 7 census/checkpoint tests: their reopen assertions
(checkpoint_verified, census re-seed, publication-proof-on-fast-path) must move to a
layer that still loads checkpoints. On V3-active stores the checkpoint fast path only
engages... where? load_active_checkpoint runs only on the non-bounded full path — for
V3-active Startup opens the bounded branch preempts it entirely. So on a V3-active
database the validated-prefix checkpoint is now DEAD WEIGHT on startup (never loaded,
only invalidated by prune). The census/checkpoint semantics live on for pre-V3 (legacy)
fixtures and via the scrub. Options per test:
  - Keep the fixture pre-V3 (don't call open_cleanly's activation — but open_cleanly is
    the only way to get ports). Alternative: after open_cleanly, the tests can still
    exercise write_validated_prefix_checkpoint/shutdown writes (those work — ports-level,
    startup_validation_clean set by activate()). Only the REOPEN assertions break.
  - For reopen assertions: assert the bounded dirty path took over
    (session.bounded_dirty_startup_path(), checkpoint not consumed) and drive the
    complete validation via the offline scrub (RedbOfflineIntegrityScrub) or background
    scrub to prove counts/census where the test's intent is "the next open verifies".
  - `the_publication_proof_survives_the_validated_prefix_fast_path`: its whole premise
    (checkpoint fast path must not skip the reactive publication proof) no longer has a
    fast path on V3-active stores. The equivalent guarantee now: the background scrub's
    complete walk must still emit the orphan finding. Adapt to drive the scrub and
    assert the finding; keep a comment naming the layer move. If the scrub does NOT
    emit it (additive counts are zeroed? no — scrub is complete path), report.
  - `a_drifted_terminal_census_is_refused_at_the_next_open`: reopen no longer loads the
    checkpoint; the refusal must now come from the scrub (complete walk) or from first
    access. The test's fail-closed intent must be preserved at the new layer — verify
    the scrub refuses (its walk verifies prefix counts). If no layer refuses anymore →
    GAP.

### D. graceful_checkpoint_close_crash_matrix (startup_graceful_close_tests.rs:351)
`assert!(session.checkpoint_verified())` on the dirty fallback reopen — that reopen is
V3-active (fixture: checkpointable_history_store + open_cleanly → activation) → bounded
dirty branch → checkpoint_verified false. The test's intent: after a failed graceful
close, the dirty reopen still reaches exact ends. Adaptation: assert
!clean_close_fast_path && bounded_dirty_startup_path() (or just drop the
checkpoint_verified assertion in favor of the bounded-path flag), keep the exact-ends
finish. The earlier loop (line 309-321) has no checkpoint assertion — only the
before-commit arm at 351 does.

### E. Follower tests (2 in storage-redb) + 33 in riffdb-server — IMPLEMENTATION GAP
Follower opens: RedbFollowerStore::open → open_mode=Follower; begin_structural_
evidence → purpose=Startup; v3_active=true (attached follower has V3 roots) → the
bounded dirty branch ENGAGES (condition checks only purpose && v3_active, not open
mode). At finish, bounded_clean_startup=true is stored, and
into_follower_after_catalog_validation (store_follower.rs:176-181) REFUSES when
bounded_clean_startup is true → InvariantViolation. This breaks:
  - startup_v3_tests.rs follower_uses_existing... (line 675) and
    newly_materialized_follower... (line 770)
  - 33 riffdb-server follower tests (open_redb_follower_startup → .unwrap() Invariant
    Violation), e.g. replication_follower_drain_tests.rs:170, reporting/receiver/staged
    authorization suites.
Also note the follower reopen path in these tests passes through
recover_follower_projections (offline-bootstrap-scrub purpose) first, which is fine;
the failure is the subsequent Startup-purpose session.
Per ADR-0182 (lines 107-108): "A follower bootstraps ... and then passes primary
startup validation at its applied head" — the follower's startup validation is the
complete validation, and the bounded dirty path text is about source readiness
("consumption of a dirty next generation per REC-004"). The bounded branch capturing
follower opens appears to be an implementation bug: either the branch condition should
exclude follower mode (is_follower_mode), or into_follower_after_catalog_validation
should not refuse on bounded_clean_startup for follower opens (the refusal exists
because bounded source startup leaves transient indexes cold and the follower path
rebuilds them itself — the follower rebuild is exactly what's being refused).
THIS IS THE PRIMARY GAP TO REPORT. Do not "fix" it by weakening the follower tests;
the two storage-redb follower tests and the server tests are currently proving the
bug. Recommendation to orchestrator: exclude follower-mode opens from the bounded
dirty branch (condition: purpose==Startup && v3_active && shared.is_source_mode()), or
equivalently keep follower startup on the complete path. That single change should turn
both storage-redb follower tests and the 33 server tests green without touching them.

### F. projection_frontier_and_marker_prefix_defects_are_derived_only (tests.rs:3632)
Fixture: initialized_store (pre-V3) + open_cleanly → V3-active → reopen takes bounded
branch → structural walk covers only bounded catalog tables (counts[0..3]) → PROJECTION_
FRONTIER/PROJECTION_APPLIED rows (phases 16/17) never inspected → findings empty →
`assert!(!findings.is_empty())` fails. The test's intent: projection defects are
derived-only findings (don't withhold ports). New layering: the complete walk (scrub)
owns population findings. Adaptation: drive the background scrub (or offline scrub) on
the damaged fixture and assert the ProjectionStateMismatch findings there; optionally
also assert the bounded startup open yields NO findings and still releases ports
(readiness not withheld by derived defects — which is exactly ADR-0182's point). Keep
req: tags.

### G. startup_republishes_once... (tests.rs:5304)
assert_ne!(second, first) fails: both checkpoint bytes identical because the second
open (V3-active, bounded) never writes a new process-generation checkpoint — the
checkpoint read at 5298 is the one written by the first activate(). On V3-active stores
the startup checkpoint write only happens on the complete path. Adaptation: this test's
semantics (one process-generation proof per complete validation, shutdown reuses it)
now live on the pre-V3/complete path. Either keep the fixture pre-V3 for this test
(checkpointable_history_store WITHOUT activation — but ports require activation...) —
no: use the scrub? The scrub writes nothing. The cleanest adaptation: after the first
open_cleanly, the second open is bounded and must NOT republish (assert_eq instead,
with a comment: the bounded path performs no complete validation so it reuses the
existing proof; the republish-once semantics apply to the complete path). But that
WEAKENS the original intent? The original intent is about complete validation
publishing exactly one proof — on V3-active stores complete validation now only runs in
the scrub (which writes nothing). The process-proof semantics remain testable on pre-V3
fixtures... but every port-releasing open of a legacy fixture activates V3. So the
"startup republishes its process proof" behavior no longer exists for V3-active stores
at all. This is a semantic change from ADR-0182 (no checkpoint on bounded path).
Adaptation: rename/refocus the test to assert the bounded reopen retains the exact
checkpoint bytes (no republish, no churn) and the shutdown write retains them — the
"publishes once" property becomes "the one published proof is stable across bounded
reopens". Flag in the report that the republish-per-process-generation behavior is now
only reachable pre-V3-activation (single generation) — semantics moved, not weakened.

## Suggested edit order (next steps)
1. Fix the follower gap FIRST (one-line condition change in startup.rs:928 area +
   possibly the clean branch at 849) — report to orchestrator as an implementation gap;
   with approval land it since 35 tests depend on it. (Task says adapt tests; but this
   is a bug the tests expose. At minimum REPORT it.)
2. startup_v3_tests.rs: counts 3→2 (line 91 stays 3? no — line 91 is the fresh-activation
   path (pre-V3 first open + activation) — that test's second finish_v3 open is on a
   V3-active store → bounded → receipts 2 not 3, epoch +1 not +3. Check line 91/95:
   fresh_v3_activation test currently PASSES (not in failure list) — because its second
   open... it IS in the passing set, leave alone. Only failing tests get edits.
3. Retention tests: shift expected counts −1, fix clean_close_fast_startup assertion.
4. Migration tests: pending GAP B decision.
5. Census/checkpoint tests: reopen assertions → bounded-path behavior + scrub-driven
   verification.
6. graceful matrix: replace checkpoint_verified with bounded-path assertion.
7. projection defects: scrub-driven findings.
8. Full suite + fmt + clippy; write final artifact.
