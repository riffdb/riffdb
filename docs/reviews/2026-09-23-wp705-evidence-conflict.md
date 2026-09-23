# WP-705 evidence-contract conflict

Recorded 2026-09-23, during the WP-705 campaign. Three blockers were found
that stop the package's evidence deliverables. Each is a conflict between
WP-705's exit gate (written 2026-09-02 through 2026-09-05) and accepted
records or removals that landed after it. Per AGENTS.md these are stop
conditions — "a required guarantee appears impossible under an accepted ADR"
and "a test reveals a conflict between the specification and an accepted
ADR" — not implementation choices under D-003.

## Blocker 1 — the clean-lifecycle evidence is not producible for any new database

**Reproduced.** The rows_65536 lifecycle run
(`RIFFDB_APP_BASELINE_WP705_LIFECYCLE_EVIDENCE=1 benchmarks/run-app-baseline
--production --organizations 4 --tickets-per-project 86 --comments-per-ticket
5 --labels-per-ticket 8 --seed-only --skip-postgres --reps 1`) fails:

```text
[riffdbd-diag] trigger=startup clean_close_fast=false decline=bounded_roots_unavailable
  (full validation pass; cost scales with retained history)
riffdb-startup-stages-v1  mode=complete_validation ... transient_index_rebuilds=1
  population_table_walk=observed ...
app-baseline failed: wp705_heap_envelope_refusal stage=process_start_to_ready
  vm_data_bytes_before=47611904 vm_data_bytes_after=1037082624
  heap_envelope_bytes=989470720 heap_envelope_ceiling_bytes=67108864
```

**Cause chain.** `changelog_v3_activation::stage_new_source` (changed in
d5bf8de25, WP-748, 2026-09-20) stages every newly-created database with the
`AuthoritativeStateCatalogV2` digest. ADR-0156 Amendment 6 (accepted
2026-09-18) rules that a V2 source "must decline that certificate and perform
complete ordinary validation … CLEAN cannot select a bounded fast startup for
a V2 source," and `clean_roots_available`
(store_changelog_lifecycle.rs:80) accepts only the V1 catalog digest — so
every new database declines clean-close fast startup with
`bounded_roots_unavailable` and takes the complete-validation path. At the
65,536-row checkpoint that pass measures ~989 MiB of VmData growth against
the harness's 64 MiB WP-705 heap envelope, and the harness refuses.

**The conflict.** WP-705's exit gate requires "clean production-scale reopen
and shutdown … bounded independently of live and retained populations," and
`scripts/wp705-lifecycle-referee` requires `mode=clean_certificate`, zero
population walks, and the 64 MiB envelope at both checkpoints. Under
Amendment 6 that evidence is producible only for a V1-catalog database, and
no production path creates V1 lineages anymore. The referee and the
lifecycle harness predate Amendment 6 (last touched 2026-09-04 and
2026-09-19) and were never reconciled to it.

**What a resolution needs.** One of, each a human decision:
1. A successor lifecycle record (which Amendment 6 itself names as the only
   route to a bounded V2 clean path) — a design plus durable-format and
   crash proofs, then the referee updated to it.
2. A V1-catalog evidence fixture: an accepted record authorizing a seeded
   V1 database for lifecycle evidence only, with the generator for it.
3. A reconciliation of WP-705's exit gate to the V2 world: the clean
   lifecycle deliverable restated as complete-validation cost disclosure
   (which ADR-0156 Amendment 6 and PERF-019's V2 qualification already
   require), with the bounded-clean claims moved to the successor record.

The 512 MiB complete-path `LimitExceeded` receipt (the WP-760 handoff) is
blocked behind the same precondition: the dirty-evidence arm begins with the
clean `restart_for_measurement`, which fails first.

## Blocker 2 — fourteen fresh-locator obligations name removed machinery

WP-705 owes fourteen obligations across six accepted records
(ADR-0205/0206/0208/0210/0213/0214). Thirteen of them name fresh-locator
coverage, arming, pre-arm preservation, or the queued seal witness — the
machinery ADR-0236 (accepted 2026-09-17, WP-787) removed. Their
implementations were built on the `codex/wp-705*` branches, which never
merged to main (they have no common ancestor with main's re-bootstrapped
history), and the machinery they prove no longer exists on main. One
obligation, OBL-0205-3 (request-scoped terminal-audit cause preservation),
survives and is now proven — see below.

At WP-705 closure, `check-adr-obligations` will fail each of the thirteen
("closing a package without its proof must fail"). No record has retired
them: ADR-0236 supersedes ADR-0197 and ADR-0235 but not the six WP-705
records. The honest exit is a retirement record on the ADR-0216 pattern
(retire each obligation whole, preserving its exact tuple, with the reason),
which needs human acceptance; D-001 permits records for discovered issues,
but the text still stops for a maintainer.

## Blocker 3 — twenty-two sibling cause-discard sites are outside ADR-0205's path authority

ADR-0205 Decision 7's cause preservation is violated not only in
`command_operations.rs` but in twenty-two sibling sites across
administration, commit, consumer, event, export, contract, primary-fence,
promotion-retry, reinstallation, query-discovery, reimport, and replication
operations: each follows a `finish()` error with an unconditional
`note_audit_failure` (subsystem), discarding the request-scoped cause. The
same single request-scoped terminal-audit failure on any of those operations
stops global readiness immediately instead of counting to eight.

ADR-0205 Decision 9 authorizes WP-705 to edit exactly
`command_operations.rs` (plus the two test paths); "a required edit
elsewhere stops the package for another exact human-reviewed amendment." The
two authorized sites are repaired and proven (below). The remaining
twenty-two need a narrow path-and-purpose amendment before they can be
touched.

## What landed despite the blockers

`ba71eb6cf` repairs the two authorized Decision 7 sites in
`command_operations.rs` and lands the OBL-0205-3 proof,
`request_scoped_command_terminal_audit_failure_preserves_threshold`
(tests/service/service_audit_orchestration.rs), with the ADR-0205 Decision 9
test-only helpers (a one-shot policy park and an audit-capacity hold) in
tests/service/support/mod.rs. The proof drives a real replay invocation
through a deterministic actor park, elapses the request deadline inside it,
and asserts the request-scoped terminal-audit failure preserves its cause,
fails only the affected invocation, leaves readiness up through the seventh
consecutive failure, stops exactly once at the eighth, and that a successful
audit receipt resets the counter. riffdb-service: 422 passed; clippy and fmt
clean; scoped acceptance 6/6.

END-006 remains unproven for this package (its restart/cycle evidence is
part of the blocked lifecycle runs).
