# RiffDB architecture- and process-level critique

**Scope:** architecture coherence and process risk, not line-level code review.
**Evidence base:** `SPEC.md` (10,606 lines), `adr/*.md` (248 records), `work_packages.yaml` (26,929 lines, 427 packages), `governance/tiers.yaml`, `docs/VISION.md`, `AGENTS.md`, `release/durable-format-manifest-v1.json`, `release/version-topology-v1.json`, `adr/obligations-outstanding.yaml`, and the repository's own scripts and git history at HEAD `706f99fc7` (2026-09-21).

Severity is assigned on consequence to the product thesis, not on effort to fix.

---

## F1 — The roadmap's load-bearing jumps have no plan, no packages, and no design (Critical)

`SPEC.md` §20 presents POC → Stage A (single-node alpha) → Stage B (replicated beta, OpenRaft) → Stage C (partitioned MVP). Stages A/B/C are the entire distance from the current state to the product described in §1.1 and §24.2.

**Evidence**

- `work_packages.yaml` defines 14 gates, `P0`–`P13`. None corresponds to Stage A, B, or C. The highest gate is `P13` "Relational operators in the candidate algebra"; `P12` is "Consolidation and availability program" (asynchronous changelog shipping, explicitly *no consensus*).
- No package in the 427 mentions Raft, consensus, partition leader, placement epoch, or tenant movement. A regex over `name` + `objective` returns only false positives ("draft", "drafting").
- No `riffdb-replication` crate exists (`crates/` has 44 crates; none is replication). No `openraft`/`raft` dependency appears in any `Cargo.toml`.
- `SPEC.md:7568` — "OpenRaft or equivalent consensus behind `riffdb-replication` only as a later amendment carrying ADR-0093 section 8's five safeguards." The consensus design is not decided; it is deferred to a future amendment.
- `SPEC.md:7585-7613` (Stage C) requires "Replicated tenant-local partition leaders", "Explicit partition routing and placement epochs", and "Tenant movement with fencing, catch-up, routing switch, and rollback plan." None of these has a package, a crate, or a design record.
- `SPEC.md:7583` Gate B requires repeated leader-loss, network-partition, snapshot-installation, and client-uncertainty tests. There is no cluster to test.

**Technical consequence.** The plan covers POC → alpha and stops. The MVP is presented as a continuation of the same roadmap but is at least two unproven architectural epochs away (consensus, then partitioning with tenant-local leaders), each of which the spec itself says requires a new accepted amendment before implementation. The distance from a single-node redb, single-writer, one-total-order engine to "replicated tenant-local partition leaders" is not a schedule risk; it is an uncosted second product. Any external reader of §20 will overestimate maturity by roughly two epochs.

---

## F2 — The governance process is the largest artifact and is growing faster than the product (High)

ADR-0181 ("Pre-Alpha Format Epoch Reset and Governance Load", accepted 2026-09-01) is the repository diagnosing this itself. Its own numbers, versus the repository 20 days later:

| Metric | ADR-0181 (2026-09-01) | HEAD (2026-09-21) | Δ |
|---|---|---|---|
| ADRs | 174 | 248 | +74 |
| `SPEC.md` lines | 9,456 | 10,606 | +1,150 |
| `work_packages.yaml` lines | 21,529 | 26,929 | +5,400 |
| Obligations ledger entries | 144 | 143 | −1 |
| Version-suffixed Rust types | 824 | 989 | +165 |
| Handbook words / pages | 190,116 / 214 | 284,491 / 258 | +94k / +44 |

**Evidence**

- `adr/0181-...md:68-101` states the diagnosis verbatim: "The process ledger has grown alongside… A ledger that only grows names the gap rather than closing it."
- ADR-0181's remedy has not landed. `release/durable-format-manifest-v1.json` still reports `alpha_epoch: 1`; the obligations ledger is still 1,337 lines / 143 entries; `scripts/check-adr-form` and `scripts/check-handbook-audience` do not exist; WP-757, WP-758, WP-759 are all still open.
- `governance/tiers.yaml:8-11` quotes a governance-cost baseline of 5.4% of changed lines. `./scripts/governance-cost --since 2026-06-01` currently exits 1 with `invalid byte sequence in US-ASCII` (a `String#split` on a non-ASCII commit subject), so the metric cannot be reproduced.
- `docs/VISION.md:7-10` names AI agents as the primary builders and claims "a finite surface is a provable surface, and a provable surface is one an agent can be taught completely in context" (`VISION.md:44-46`). The invariant set is now 786 unique requirement IDs, 248 ADRs, 989 version-suffixed types, and a 284k-word handbook.

**Technical consequence.** The process designed to bound complexity is not bounding it. The "one reset" was declared on 2026-09-01 and the growth rate continued through 2026-09-21. For the stated builder population — agents that must hold the invariant set in context — the surface is already past the point where "taught completely in context" is credible. This is a technical liability, not a documentation complaint: an agent that cannot hold the invariants will produce changes that satisfy the local package and violate a distant guarantee.

---

## F3 — The obligations ledger is a standing exception to the "proof must exist" rule (High)

`AGENTS.md` states: "an ADR obligation's proof MUST exist in code, not in prose" and "`./scripts/check-adr-obligations` … fails once the owning package closes without it."

**Evidence**

- `./scripts/check-adr-obligations` exits **0** while printing: `146 deferred obligation(s) across accepted records; 143 acknowledged outstanding, 0 undeclared` and `188 declared across accepted records; 119 proven, 69 outstanding`.
- `adr/obligations-outstanding.yaml` is 1,337 lines with 143 `- key:` entries, each carrying `reason: Predates scripts/check-adr-obligations. Not yet linked to a named proof.`
- ADR-0181 §4 requires the ledger to "reach zero and the file is removed" and the checker to "reject any ledger file or entry." The file is present and the checker accepts it.
- ADR-0181's own obligations are outstanding. `OBL-0181-5` names the proof `check-adr-form`; `OBL-0181-6` names `check-handbook-audience`. Neither script exists (`ls scripts/check-adr-form` → No such file). The checker reports them as outstanding and still exits 0.

**Technical consequence.** The machine check that is supposed to make obligations falsifiable is a no-op for 143 acknowledged plus 69 v2-outstanding obligations. The guarantee "a proof exists in code" is asserted in `AGENTS.md` and not enforced by the tool that `AGENTS.md` names as the enforcement. A package can close with its obligations undischarged and CI stays green.

---

## F4 — Spec drift: amendment text bolted into §4.10, and the document no longer describes itself (High)

**Evidence**

- `SPEC.md:1219-1938` — §4.10 "Consolidation and availability program" is 720 lines (6.8% of the spec) of amendment prose with acceptance dates in the headings: `### Follower columnar views (ADR-0178 amendment accepted 2026-09-15)` (line 1300), `### Canonical vectors in V2 (ADR-0160 amendment accepted 2026-09-15)` (1358), `### Archive restore receipt V3 (ADR-0050/0178 amendment accepted 2026-09-15)` (1404), `### Exact command-prefix restoration evidence (accepted 2026-09-15)` (1500).
- The revision-history table (`SPEC.md:36-173`) has ~100 rows, several of which are multi-paragraph essays (v1.20 at line 52, v1.29 at line 43). ADR-0181 §5 explicitly requires a check that "fails on a `SPEC.md` document-control row longer than one paragraph." That check does not exist.
- The header says `**Version:** 1.30`, `**Date:** 2 September 2026` (`SPEC.md:9-11`), but the body contains accepted amendments dated 2026-09-15/16/18 and ADRs dated through 2026-09-21. The document's own version and date no longer describe its content.

**Technical consequence.** The authoritative specification is a layered accretion. A reader — human or agent — cannot distinguish current normative text from historical amendment without reading the whole document. The document-control table, which is supposed to be the index, is itself unbounded and contains normative-sounding prose. This is the mechanism by which F2's growth becomes a correctness risk: the spec is the source of truth and it is no longer navigable.

---

## F5 — Accepted decision records can exist on unmerged branches and vanish from mainline (Medium-High)

**Evidence**

- ADR numbering gaps: **0067, 0068, 0069, 0224, 0225**. `ls adr/` goes 0066 → 0070 and 0223 → 0226.
- 0067–0069: no file, and no reference anywhere in `SPEC.md` or `adr/`.
- 0224/0225: git contains `041e3e2f4 docs: record human acceptance of ADR-0224` and `085a8b1e4 governance: record exact human acceptance of ADR-0225`. `git merge-base --is-ancestor 041e3e2f4 HEAD` → **NO**. Those commits live only on branches `codex/adr0226-0227-proposals` and `codex/wp757-integration-safe`. The files are absent from HEAD and `adr/README.md` does not mention them.
- The commit message for 041e3e2f4 records a human acceptance with a body SHA256 — i.e. a decision the maintainer accepted, which is not in the mainline tree.

**Technical consequence.** "Accepted" is not a durable state. The ADR number is not a reliable index of what was decided, and a human acceptance can be recorded on a branch that never merges. For a repository whose entire governance model rests on "an accepted record is authoritative," this is an integrity gap: the authoritative set is whatever happens to be on the current branch, and the numbering does not reveal what is missing.

---

## F6 — The vision's "one process, one backup unit" story is broken by the replication tier and follower columnar views (Medium-High)

**Evidence**

- `docs/VISION.md:29-31`: "Committed means indexed means deliverable, inside one total order, behind one typed freshness vocabulary, **in one process with one backup unit**."
- `SPEC.md:7558-7583` (Stage B) adds follower mode, capability-guarded streaming, incarnation-fenced promotion, a follower retention fence, and a changelog-derived archive sink.
- `SPEC.md:1300-1356` ("Follower columnar views", accepted 2026-09-15) makes a follower's columnar view "a separately validated, rebuildable local derivation" that "may materialize its view independently of source artifact readiness," with its own process generation and local view identity.
- `SPEC.md:7585-7613` (Stage C) adds "Replicated tenant-local partition leaders" and "Tenant movement with fencing, catch-up, routing switch."
- `grep -n "follower\|partition\|replicat" docs/VISION.md` returns **nothing**. The vision page was not updated when the availability program was accepted.

**Technical consequence.** The single-process/single-backup-unit claim is true only for the POC. The MVP is a multi-process, multi-copy system with per-follower derived state and partition movement. The pitch that connects the ADRs (`VISION.md:67-70` says "This page is only the argument that connects them") now connects a different system than the one the ADRs describe. Either the vision is stale or the architecture has outgrown its thesis; the repository does not say which.

---

## F7 — The scope fence holds on SQL and distributed transactions, but the query surface is expanding faster than the fence language (Medium)

`AGENTS.md:23` (boundary 10): "Do not add SQL, Raft, distributed transactions, arbitrary transaction callbacks, or general analytical joins to the POC critical path."

**Evidence — the fence is holding where it is structural**

- No SQL surface exists; `SPEC.md:7602` makes read-only SQL optional and explicitly not an MVP gate.
- Distributed transactions are explicitly post-MVP (`SPEC.md:7606`, `SPEC.md:221`).
- No Raft in the POC (`SPEC.md:300`, `REP-001`).

**Evidence — the fence is eroding where it is a judgment call**

- RiffQL has grown to 126 `OQ-*`/`RQL-*` requirements (`OQ-001` through `OQ-118`). §24.5.4 alone is 553 lines.
- ADR-0185 (accepted 2026-09-02) adds relational operators — one-to-many expansion, `exists`/`not exists` — to the candidate algebra. Its own context (`adr/0185-...md:68-80`) says: "Since ADR-0130 every new operational read shape has landed as a provider family with its own descriptor, epoch semantics, cursor family, provider-state version, and service port: exact text, exact predicate, long pattern, tokenized text, projection result sets. That pattern is how the workspace reached 824 version-suffixed types and seven per-engine service ports."
- `SPEC.md:272` still lists "Arbitrary joins, windows, distinct counts, or approximate analytics" as POC non-goals, while ADR-0185 admits bounded joins and expansion. The distinction is "bounded" vs "arbitrary" — defensible, but it is now decided per-ADR rather than by a structural limit.

**Technical consequence.** The "no SQL" fence is real and load-bearing. But the non-goal list is stale relative to accepted ADRs, and the query surface is growing by provider family faster than the fence vocabulary can track. Each new family is a subsystem (descriptor, epoch, cursor family, provider state, service port), which is exactly the complexity ADR-0185 was written to stop — and ADR-0185 itself adds two operators. The fence needs a structural test (e.g. "no new provider family without a written operator refusal", which ADR-0185 §1 states) enforced by a check, not by reviewer memory.

---

## F8 — The "average multi-tenant SaaS" pitch is not matched by the contract surface (Medium)

**Evidence**

- `docs/VISION.md:3`: "RiffDB is the data layer for **the average multi-tenant SaaS application**."
- The actual surface: 786 unique requirement IDs, 989 version-suffixed Rust types, 44 crates, ~932k lines of Rust, 5,265 tests.
- `docs/known-limitations.md` is 287 lines of refusals: no general joins, no canonical-string ranges, no multiple branching dimensions, no residual post-page filters, no index intersections, no caller-selected plans, no depth-two expansion, no cross-partition joins, no recursion, no runtime join optimization, no general SQL, no arbitrary transaction callback, no analytical join engine, no distributed transaction, no automatic failover, no consensus.
- An application author must learn a contract DSL, RiffQL, named operations, generated bindings, a lock/manifest system, a migration ceremony, a capability model, a freshness vocabulary, and the refusal vocabulary.
- The agent-application campaigns show good *sealed* results (v2: 4 runs, 9.2–9.4; v3: 6 runs, 8.7–9.7), but these are the maintainer's own harness. Campaign 03's time-to-first-committed-row was **11, 316, 600, and 1,680 seconds** (`adr/0181-...md:99-101`) — a 150× spread with the slowest cell at 28 minutes.

**Technical consequence.** The design is a narrow, high-discipline, provable surface. That is a legitimate product — but it is not "the average multi-tenant SaaS application." An average team (or its agent) will hit refusals that require recompiling contracts, and the 150× spread in the campaign's own time-to-first-row is the leading indicator. The honest claim is "a narrow, provable surface for teams willing to adopt the contract model," and the pitch should say so. Overstating reach is a product risk that shows up as churn, not as a bug.

---

## F9 — The claims that matter most for the thesis have no falsification path (Medium)

**Evidence**

- `SPEC.md:7981-8001` (§24.1 POC definition of done) requires: "The architecture review records one of three decisions: proceed to alpha, revise and repeat a gate, or stop because the core thesis was not demonstrated." WP-200 ("POC acceptance and release") has **no closure block** in `work_packages.yaml`, and no architecture-review artifact exists in the repository.
- `SPEC.md:8003-8014` (§24.2 MVP definition of done) requires replication/partition semantics under failure, an independent security review, and sustained-load SLOs. None has a package (see F1).
- ADR-0181's obligations — epoch-2 reset, ledger closure, handbook rewrite measured by campaign 04 — are all outstanding. `evaluations/agent-application-alpha/campaigns/` contains campaign-01/02/03 only; **campaign-04 does not exist**.
- The performance gate has been amended repeatedly: 37 ADRs mention `PERF-008`. 27 packages carry non-clean closure statuses, including 4 `*failed_performance_gate` and 8 `complete_without_production_activation`. `SPEC.md:70` (v1.02) records that the full TicketDesk seed was moved from an alpha parity gate to a "receipted evidence under a 5.0x same-run regression ceiling" because "the measured single-lane ordered apply/submit floor alone exceeds the seed parity budget."

**Technical consequence.** The claims that carry the product thesis — replication, partitioning, sustained load, independent security review, and the POC's own "is the thesis worth productizing" decision — have no evidence artifact and, in several cases, no package. The performance gate is being adjusted to the measurements rather than the measurements meeting the gate; that is a legitimate engineering response, but it means the gate is not currently a falsifier. The repository is strong on *unit-level* falsifiability (5,265 tests, crash matrices, simulation) and weak on *thesis-level* falsifiability.

---

## F10 — The governance system's own instrumentation is incomplete, and one check is broken (Medium)

**Evidence**

- `governance/tiers.yaml:97-100` runs, for `adr/**, SPEC.md, work_packages.yaml, governance/**`: `check-requirement-coverage`, `check-adr-obligations`, `adr-new --self-test`, `adr-index --check`, `check-acceptance-commits`, `check-performance-freeze`. It does **not** run `check-adr-form` or `check-handbook-audience` — the two checks ADR-0181 §5–6 requires. Neither script exists.
- `./scripts/governance-cost` exits 1 with `invalid byte sequence in US-ASCII` on the current git log, so the 5.4% governance-share baseline quoted in `tiers.yaml:8-11` cannot be reproduced.
- Tier assignment (`tiers.yaml:21-85`): `crates/riffdb-commit/src/**`, `riffdb-conflict`, `riffdb-idempotency`, `riffdb-auth`, `riffdb-policy`, `riffdb-runtime`, `riffdb-invariant` are `guarantee`. `adr/**` and `SPEC.md` are `surface`. A change to the authoritative specification therefore requires only a short-form record and fixture-diff review, while a change to the code it governs requires exact-text human acceptance.

**Technical consequence.** The process's own instrumentation is unreliable (a broken metric script) and incomplete (two required checks absent). More importantly, the tier assignment inverts the intended risk ordering: the documents that define the guarantees are easier to change than the code that implements them. Given F4 (spec drift) and F5 (accepted records on unmerged branches), the `surface` tier for `SPEC.md` and `adr/**` is the mechanism by which the authoritative set drifts without the ceremony the guarantees receive.

---

## Summary

| # | Finding | Severity |
|---|---|---|
| F1 | Roadmap's Stage A/B/C have no packages, no crate, no design; MVP is uncosted | Critical |
| F2 | Governance artifact growing faster than product; ADR-0181's own reset did not land | High |
| F3 | Obligations ledger is a standing exception to "proof must exist"; checker exits 0 | High |
| F4 | Spec drift: §4.10 amendment accretion; document no longer describes itself | High |
| F5 | Accepted ADRs exist on unmerged branches; numbering gaps 0067–0069, 0224–0225 | Medium-High |
| F6 | "One process, one backup unit" broken by replication tier and follower columnar views | Medium-High |
| F7 | Scope fence holds on SQL/distributed-tx; query surface expanding per-ADR | Medium |
| F8 | "Average multi-tenant SaaS" pitch not matched by a narrow, refusal-heavy surface | Medium |
| F9 | Thesis-level claims (replication, partitioning, security review, POC go/no-go) unfalsified | Medium |
| F10 | Governance instrumentation incomplete; `governance-cost` broken; tier ordering inverted | Medium |

**The through-line.** RiffDB's *unit-level* engineering discipline is genuinely strong: 5,265 tests, crash matrices, deterministic simulation, byte-exact compatibility fixtures, and a real follower implementation. The weaknesses are all at the *system and process* level: the plan stops at alpha while the spec promises an MVP two epochs further; the governance apparatus that is supposed to bound complexity is itself the fastest-growing artifact and its own reset did not land; and the checks that would make the process falsifiable are either absent, broken, or exit 0 in the presence of acknowledged gaps. The repository is better at proving small things than at proving the thesis, and the process is now large enough to be a technical risk in its own right.
