# September 21 design review disposition

Disposition of the twelve findings in
`reviews/2026-09-21-design-review-synthesis.md` and its four sub-critiques,
each checked against this checkout before being accepted or rejected. Base
commit `706f99fc`.

One finding is a reproduced soundness defect and is fixed. Three claims are
rejected: the mechanism they describe is not what the code does. Two are
accurate restatements of the project's own published numbers or recorded scope
decisions. The rest are documentation gaps, now closed, or remain open with an
owner named here.

Verifying the critical finding surfaced a second defect the review did not
report, and a governance gap that explains how both shipped.

## Individual findings

| # | Disposition | Evidence and action |
|---|---|---|
| S-1 | Fixed | Reproduced: two aggregates sharing a partition route, same writes and conflict keys, compile to `commit_checks=0` in one binding order and to checks in the other. `lower_locality` anchors on the first mutating binding, commit-check lowering derived aggregate invariants from that anchor, and the coverage proof took its expected set from the same anchor. The runtime is not a backstop; `command_validation.rs` returns `NonZero` without evaluating when the list is empty. ADR-0252 decides that the anchor governs route and conflict ownership only. Two tests in `cross_aggregate_writes.rs` fail if the lowering is reverted. |
| S-1b | Fixed; not reported by the review | The mirror defect in the other binding order: the root-validation target loop accepted key arguments from bindings of *other* aggregates, so an invariant was evaluated against a root key fabricated from a foreign aggregate's key prefix — a record the command never writes. Same record and fix. |
| S-2 | Accurate; boundary now documented | A command reads by complete key, an invariant may not span aggregates, and a command cannot read a projection. `docs/known-limitations.md` now states the boundary, the patterns it excludes, the read-choose-then-command idiom and that the selection happens outside the command, and that R-01's "durable workflows outside commands" has no facility behind it. Whether to fund one is a scope decision, not a defect. |
| S-3 | Accurate in substance, mis-framed | The absence of Raft packages is not an oversight: `REP-001` requires the POC to preserve replicated-state-machine-shaped boundaries and *not* implement Raft, and `R-13` registers the retrofit risk with a named mitigation. What is true is that no MVP-epoch design exists and R-13's mitigation is asserted rather than proven. Open; it is a scope decision for the POC-exit review, which `POC-001..010` already gates. |
| S-4 | Accurate; already published | The 0.17–0.22x figures and the 7.6x write gap are this project's own measurements in `performance/app-baseline-production-profile.md`, published there before the review. Nothing to correct. The go/no-go reading remains an input to the POC-exit decision. |
| S-5a | Rejected: an escape exists | `min_projection_durable_frontier` skips projection ids in the `detached` set, `riffdb retention projection-detach --projection-id --reason` is the operator surface, and columnar controls attach retention only while `Ready` with budget-triggered detachment. A stuck projection does not pin the fence with no way out. |
| S-5b | Fixed; characterisation corrected | Vector ANN does not silently disengage above 500 rows. `merged_org_bounded` returns `None` once merged rows exceed the budget, so an organization above the ceiling gets a typed `ScanBudgetExceeded` refusal. The real defect is the inverse: ANN engages only *above* the contract's declared `row_threshold`, so a declared threshold of 500 or more can never engage at all, and `recall_target_bps` is unmeasured in production. `known-limitations.md` said such a threshold "remains exact"; it now states both consequences, and the literal 500 is a named constant carrying them. |
| S-5c | Accurate; consequence overstated | The second substrate is real: `exact_text_adapter.rs` persists checkpoints with write, `sync_all`, rename and a parent-directory sync, outside redb, and the backup artifact copies only the database file, the journal and the format marker. But a text checkpoint is derived state, which boundary 8 requires to be rebuildable, so its absence from a backup is by design rather than a durability hole, and each checkpoint carries a `history_incarnation` that fences a stale one after a restore. The recovery-validation cost claim is a performance assertion that needs measurement, not reading, and is not dispositioned here. |
| S-6a | Rejected | `check-adr-obligations` exiting 0 with outstanding obligations is its designed contract, not a broken remedy: it exits 1 when an outstanding obligation's owning package is closed (`scripts/check-adr-obligations:354`, self-tested at `:703-708`). 69 obligations on open packages is the expected state. |
| S-6b | Rejected | `governance-cost` is not broken. It runs, reports 4.0% of changed lines as governance, and exits 0. |
| S-6c | Fixed; review's own figure stale | Four handbook pages labelled current rules "grammar-v1". The shipped maximum is V23, not the v20 the review names, and SPEC does record V23. The label, not the rule, was wrong: the cookbook and SPEC §7.2 now explain least-sufficient versioning, under which a contract using only the base constructs keeps V1 identity byte for byte. |
| S-6d | Fixed | ADR-0224 and ADR-0225 are accepted guarantee-tier records living only on `codex/wp757-integration-safe`, owned by the still-open WP-757. A generated index cannot show an absent number, so nothing on main said so. `adr-index` now refuses an undeclared gap, and `adr/RESERVED.yaml` declares them. The check found three more the review did not: 0067–0069, never written in any branch or commit. ADR-0225 also carries no `acceptance:` reference despite its date, so `check-acceptance-commits` will refuse it; the reservation says so where whoever merges WP-757 will read it. |
| S-6e | Fixed in part | "Authoritative docs are surface tier while the code they govern is guarantee tier" understates it. `governance/tiers.yaml` itself classified as **internal** under the catch-all rule: the table assigning every path its ceremony required none of its own, so any tier could be lowered by a change needing no record. ADR-0253 raises the table, and raises command lowering and the plan coverage proofs to sit alongside `riffdb-invariant`, which evaluates what they decide. That gap is how S-1 shipped. |
| S-7 | Fixed; claim overstated | `ADB-C042` has no implementation, but it is not "R-03's flagship mitigation": R-03 names compiler dependency checks, read revalidation, an adversarial corpus and fail-closed behavior, never C042. The condition it describes is unreachable — a conflict-key component may name only an aggregate-root key field, so every conflict key is a function of the root key and two writes to one root always serialize. ADR-0254 records this as `CMP-014` under WP-805, SPEC now prints a real `RDB-C016` diagnostic, and four tests enumerate the categories of name a component may hold so a widening grammar fails loudly. |
| S-8 | Accurate; accepted trade | ADR-0012 §377 states it outright: an `Integrity` fault leaves a mutating admission pending for operator intervention and the POC provides no online repair path, deliberately, because terminalizing it would hide corrupt plan or snapshot evidence. So the key is not "burned with no operator path" -- it is held *for* an operator, with no online tool yet. The attempt budget and restore incarnation fence were not re-verified. |
| S-9 | Intentional and documented | `LOG-001` sequences persisted no-op business outcomes deliberately, and SPEC states the reason: it unifies committed outcomes, provenance, change consumption and projection frontiers. The cost is real; it is a recorded trade, not an oversight. |
| S-10 | Fixed in part; rest already documented | No compaction is accurate and was undocumented: redb 4.2 exposes `Database::compact` and nothing calls it, so retention pruning frees pages inside the file and never lowers its high-water mark. `known-limitations.md` now says capacity planning must size for the historical peak. The dirty-restart `LimitExceeded` at the 512 MiB historical-evidence ceiling is accurate and was already published on that same page. The backup receipt location was not re-verified. |
| S-11 | Accepted as written | The MCP surface assessment matches the code, and the catalog-size risk having no evidence from real models is a fair statement of R-06's status. No action. |
| S-12 | Accurate; author's call | Confirmed: `VISION.md` claims "one process with one backup unit" and the words follower, replica and partition do not appear in it, while the replication tier and follower columnar views are planned and recorded elsewhere. VISION is a product thesis, not a normative file, so reconciling the claim is a decision about what to promise rather than a defect to repair. Left for the author. |

## What changed

- `adr/0252` and the lowering fix, with two load-bearing tests.
- `adr/0253` and `governance/tiers.yaml`.
- `adr/0254`, `CMP-014`, `WP-805`, four conflict-key tests, and the SPEC
  diagnostic example.
- `scripts/adr-index` numbering guard and `adr/RESERVED.yaml`.
- `docs/known-limitations.md`: the expressiveness boundary, storage
  reclamation, and the corrected vector ceiling.
- Four handbook pages and SPEC §7.2 on grammar versioning.

## Review boundary

Nothing here changes transaction ordering, conflict ownership, read
validation, atomicity, or outcome sequencing. ADR-0252 moves the plan hash of a
cross-aggregate command whose non-anchor aggregate declares invariants; that
plan was unsound, and single-aggregate identities are unchanged, which the
compatibility fixtures confirm. No durable encoding, storage key, protocol, or
MCP behavior changes.
