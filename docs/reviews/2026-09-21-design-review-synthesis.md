# RiffDB design review — synthesis

Date: 2026-09-21 · Scope: product design and vision, technical items only.
Inputs: five deep-dive critiques (four completed, one covered by the foreman),
each grounded in SPEC.md, adr/, docs/, and crates/. Sub-critiques:

> **This is the review as received, kept unedited as the primary source.** Its
> claims were checked afterwards and three of them are wrong: the obligations
> checker and `governance-cost` behave as designed, and the projection
> retention fence has an operator escape. The shipped grammar is V23, not the
> v20 stated below. See `reviews/2026-09-21-design-review-disposition.md` for
> what was verified, fixed, or rejected.

- `docs/reviews/2026-09-transaction-concurrency-idempotency-critique.md`
- `docs/reviews/2026-09-contract-dsl-compiler-riffql-critique.md`
- `docs/reviews/storage-projection-durability-critique.md`
- `docs/reviews/architecture-process-critique-2026-09-21.md`

## Verdict

The safety architecture is real and unusually disciplined where it applies:
command-only writes, atomic commit graph (mutations + outcome + events +
provenance + sequence), idempotent replay, fail-closed everywhere, honest
self-measurement. But the review found one critical soundness defect in the
compiler, one structural expressiveness gap at the center of the product bet,
and a roadmap whose MVP epoch has no design at all. The vision's "average
multi-tenant SaaS" pitch is not yet supported by the surface it ships.

## Critical

**S-1. Aggregate invariants are silently dropped by binding order.**
`lower_locality` (command_lowering.rs:1416-1436) anchors on the *first*
mutating binding; `lower_commit_checks` (:1576-1585) lowers aggregate-level
invariants only for that anchor aggregate; and the IR coverage proof
(`validate_commit_checks`, plan.rs:7092-7253) validates against the same
anchor, so it cannot catch the omission. Empirically reproduced: a command
with `mutate Other(...)` then `mutate Child(...)` compiles with
`commit_checks=0` where the reordered form yields `commit_checks=2` — same
writes, same conflict keys, different semantics. This violates POC-003 and is
risk R-03 realized inside the compiler's own proof, not the author's
declaration. ADR-0170 (cross-aggregate writes, IR V19) created the shape; the
invariant lowering was never extended to cover it.

**S-2. The expressiveness wall sits exactly where the safety claim stops.**
Commands read by complete key only (SPEC.md:7738 rejects write-influencing
range reads); invariants cannot span aggregates (SPEC.md:2487); commands
cannot read projections (SPEC.md:2512-2519). Slot allocation, credit limits
across entities, rollups, and computed status are inexpressible. The
documented mitigation — "use durable workflows outside commands"
(SPEC.md:7632) — appears exactly once in the repo; no ADR, requirement, or
work package exists for it, and sagas are post-MVP (SPEC.md:7617). The actual
workaround (application reads a page, picks a target, then invokes a command)
is explicitly excluded from the safety claim (adr/0055:214-220). Net effect:
for a large class of ordinary SaaS patterns, RiffDB relocates the unsafe
pattern into application code — the exact pathology the vision
(docs/VISION.md:19-27) claims to remove by construction.

## High

**S-3. The MVP epoch is undesigned.** 427 work packages through gate P13
cover POC→alpha and stop. No package, crate, ADR, or dependency exists for
Raft/consensus, partition leaders, placement epochs, or tenant movement.
Worse, the changelog is architecturally a *derived view* of durable state
(ADR-0093 §1; ADR-0178 §8 refuses acknowledgement changes), so retrofitting
consensus requires inverting what is authoritative — a durability-architecture
rework, not "OpenRaft behind a facade" (SPEC.md:7568).

**S-4. Measured performance contradicts the integrated-database thesis.**
RiffDB's own comparison harness shows 0.17–0.22x Postgres overall
(app-baseline-production-profile.md:104-119), with writes 7.6x slower at p50
(:134-141). The gap is the semantic write path itself (provenance, audit,
journal, canonical encoding), not the storage engine — so it is a
go/no-go input under the project's own rule (SPEC.md:262). Hot-key
serialization is structural (every `AddArtifact` in the museum example
serializes on one `collection_id`), and the escrow/optimistic escape (R-02)
is refuted by ADR-0170's own measurements and in tension with ADR-0094.

**S-5. The projection plane has unbounded-failure modes.** One poison
projection pins the *global* retention fence (retention.rs takes the min over
all controls; STO-030), so commits/events/outbox grow without bound while it
serves nothing, with no automatic escape. Recovery validation is
O(frontier² × groups) in commit re-evaluations. Vector ANN silently disengages
above 500 admitted rows while the contract permits a declared threshold of
65,536 — the recall contract (VEC-010) is unmeasurable in production. FTS
writes a second durability substrate (whole-partition file + rename + fsync)
outside redb and outside the backup artifact.

**S-6. The process is now a technical liability for its own thesis.**
249 ADRs (~68k lines), a 10.6k-line spec, 284k-word handbook, 20 grammar
versions — against the vision claim that a surface is "one an agent can be
taught completely in context" (VISION.md:45-46). ADR-0181 diagnosed this
(2026-09-01) and its remedies did not land: the obligations checker exits 0
while reporting 69 outstanding obligations; `governance-cost` is broken;
SPEC §7 still describes grammar v1 while the shipped parser is v20; §18.5
contradicts accepted ADR-0072; ADRs 0224-0225 are accepted only on unmerged
branches. Authoritative docs are `surface` tier while the code they govern is
`guarantee` tier — the spec is easier to change than the implementation.

**S-7. R-03's stated mitigation does not exist.** The flagship diagnostic
`ADB-C042` ("invariant is not protected by the declared conflict key",
SPEC.md:2744-2751) has no implementation — no compiler pass relates invariant
dependencies to conflict-key components. Combined with S-1, invariant
protection currently rests on nothing the compiler proves.

## Medium

- **S-8.** Idempotency edge cases: an `Integrity` fault permanently burns the
  idempotency key with no operator path; the attempt budget is
  invocation-local, so total work per identity is bounded only by caller
  patience; the restore incarnation fence (R-16) is opt-in and the idempotency
  identity carries no incarnation.
- **S-9.** LOG-001 sequences persisted business rejections into the same log
  as real commits; the "separately ordered outcome journal" mitigation (R-05)
  is unavailable under LOG-001 as written.
- **S-10.** Storage hygiene: no compaction anywhere under
  riffdb-storage-redb (the file high-water mark is permanent); dirty restart
  fails closed at the 512 MiB historical-evidence ceiling at the production
  benchmark population (known-limitations.md:237-245); the backup receipt is
  a single copy in `backup_root/.maintenance`, not in the artifact.
- **S-11.** MCP/agent surface: well-designed (capability-filtered discovery,
  fresh authorization per call, redacted single-violation diagnostics,
  idempotency-key discipline), but the catalog-size/stability risk (R-06) has
  no evidence from real models, and the "learnable from error messages at
  runtime" claim is aspiration — the runtime diagnostic surface is thin
  compared with the deploy-time surface.
  Follow-up: the [RDB-C048 natural authoring case](https://github.com/riffdb/riffdb/blob/main/evaluations/agent-authoring/README.md)
  now records one observed adapter migration and controlled compiler replays.
  It separates four false positives from 13 structurally unguarded writes;
  it supplies no MCP catalog-selection measurement and leaves R-06 open.
  A subsequent [fresh-context pilot](https://github.com/riffdb/riffdb/blob/main/evaluations/agent-authoring/fresh-context-2026-09-22/README.md)
  adds two successful compiler-repair participants and eight catalog-selection
  participants, with 48/48 correct first responses at 10/66 visible tools.
  Catalogs came from real capability-filtered discovery. The model tasks used
  file-assisted inspection and explicit simulated refresh, so native-client
  behavior, autonomous notification handling and broader R-06 closure remain
  unqualified. This replaces the absence-of-real-model-evidence finding only
  within those recorded conditions.
- **S-12.** Vision contradictions: "one process, one backup unit"
  (VISION.md:29-31) is broken by the replication tier and follower columnar
  views; VISION.md never mentions followers, partitions, or replication.

## What is sound (stated briefly)

Commit-time revalidation ordering; the single-terminal-row idempotency shape
with startup reciprocity checks; crash-between-reservation-and-commit
handling; deterministic runtime isolation; the MCP authorization gates
(fresh per-call authorization, secret-field gating before ordinary
visibility); ADR-0015 mandatory binding outcomes; the honest
known-limitations page and self-published negative performance results.

## Highest-leverage fixes

1. Fix S-1 and add a binding-order invariance regression test (semantics
   must not depend on declaration order).
2. Implement ADB-C042 or delete the claim (S-7).
3. Decide the selection/workflow story: either specify durable workflows or
   admit the expressiveness boundary in the vision (S-2).
4. Write the Stage B/C architecture ADR before more alpha packages land, or
   re-scope the MVP claim (S-3).
5. Land ADR-0181's governance remedies (S-6).
