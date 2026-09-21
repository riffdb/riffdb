---
adr: "0253"
title: Enforcement Decisions And The Tier Table Are Guarantee Tier
status: accepted
tier: guarantee
date: 2026-09-21
accepted: 2026-09-21
acceptance: 'maintainer, in session, 2026-09-21: "you have permission to write and
  accept your own ADRs for this session without a human in interest of advancing
  these findings" (ADR-0253 as written), after ADR-0252 showed the gap'
requires: [ADR-0252]
amends: []
supersedes: []
requirements: []
packages: []
obligations: []
review_triggers:
  - Any change to governance/tiers.yaml.
  - Any proposal to lower a path's tier.
---
# ADR-0253: Enforcement Decisions And The Tier Table Are Guarantee Tier

## Context

`governance/tiers.yaml` places `crates/riffdb-invariant/src/**` at guarantee
tier: the crate that *evaluates* a commit check is ceremonially protected. The
code that decides whether a commit check exists at all -- command lowering in
the contract compiler, and the IR coverage proof that is supposed to catch a
lowering that emits too few -- sat at surface tier under the blanket
`crates/riffdb-contract-compiler/**` and `crates/riffdb-contract-ir/**` rules.
ADR-0252's defect shipped through that gap. Evaluating the right predicate
perfectly is worth nothing if the predicate was never lowered, so the two
belong at the same tier.

The table itself is worse. `./scripts/classify-change governance/tiers.yaml`
returns `internal`, matched by the catch-all `**` rule: the file that assigns
every other path its ceremony requires none of its own. Any tier in the
repository can be lowered by a change that needs no record and no review, which
makes every other rule in the file advisory. `AGENTS.md` is already guarantee
tier for the same reason; `governance/tiers.yaml` was missed.

## Decision

1. The paths that decide what a command enforces are guarantee tier:
   `crates/riffdb-contract-compiler/src/command_lowering.rs`,
   `crates/riffdb-contract-ir/src/plan.rs`, and
   `crates/riffdb-contract-ir/src/plan/**`.
2. `governance/tiers.yaml` is guarantee tier. The table that assigns ceremony
   may not be weakened by a change that requires none.
3. The remainder of the contract compiler and contract IR crates stay at
   surface tier. This record raises the enforcement decision, not grammar,
   diagnostics, code generation, or bundle assembly.

## Options considered

1. **Raise `crates/riffdb-contract-compiler/**` and
   `crates/riffdb-contract-ir/**` to guarantee.** Rejected. Grammar, diagnostic
   text, generated artifacts, and docs output dominate both crates. Requiring a
   full record for a diagnostic wording change makes the guarantee tier routine,
   and a ceremony that fires constantly stops carrying information.
2. **Leave the tiers as they are and rely on review to catch this class.**
   Rejected: that is the process that was in effect when ADR-0252's defect
   merged.
3. **Raise the specific enforcement-deciding files, and the tier table.**
   Chosen. It is the smallest change that makes the ceremony match where the
   guarantee is actually decided, and it closes the recursion by which the table
   could be edited away.

## Consequences

- A change to command lowering, the executable plan, or the coverage proofs
  needs a full record and human review. These files change rarely; ADR-0170 and
  ADR-0252 are the recent examples, and both warranted a record.
- Lowering any path's tier now needs an accepted record naming the reason.
  Raising one, and adding a path, need the same -- the rule does not
  distinguish, because "which paths the table covers" is the same decision as
  "what ceremony they carry".
- `./scripts/classify-change` takes the maximum over touched paths, so a change
  that edits command lowering alongside ordinary compiler code is guarantee
  tier. That is the intended reading: the enforcement decision dominates.

## Standing design tests

- **Interface safety:** unchanged. This record moves ceremony, not surface; no
  application-visible behavior depends on it.
- **Scale:** unchanged. Tier classification is a path match over the touched
  files of one change.

## Checks

- `./scripts/classify-change --require surface governance/tiers.yaml` exits 1.
- `./scripts/classify-change --require surface
  crates/riffdb-contract-compiler/src/command_lowering.rs` exits 1.
- `./scripts/classify-change --require surface
  crates/riffdb-contract-ir/src/plan/commit_coverage.rs` exits 1.
