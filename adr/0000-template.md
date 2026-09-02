---
adr: 0000                    # four digits; `./scripts/adr-new` allocates it
title: Short Title In Title Case
status: proposed             # proposed | accepted | rejected | superseded
tier: surface                # surface | guarantee; internal changes need no record
date: YYYY-MM-DD             # the day the record was written
accepted: null               # the date a human accepted it; acceptance is a human act
requires: []                 # [ADR-0093, ...] records this one depends on
amends: []                   # [ADR-0093, ...] records this one refines
supersedes: []               # [ADR-0093, ...] records this one replaces
requirements: []             # [REP-002, ...] SPEC requirement IDs this record binds
packages: []                 # [WP-746, ...] packages that implement it
obligations: []              # each entry: id, package, proof, says (see below)
review_triggers: []          # each entry: one sentence naming a change that needs review
---
# ADR-NNNN: Short Title In Title Case

<!--
Body caps: surface tier at most 120 lines; guarantee tier may add
`## Options considered` and `## Consequences` and is capped at 200 lines.

The front matter is the single source. Index rows, package skeletons,
obligation tracking, and requirement links are derived from it, so state a
fact once, there, rather than repeating it in prose. An obligation entry is:

obligations:
  - id: OBL-NNNN-1
    package: WP-NNN
    proof: test_function_name        # or scripts/<name>
    says: One sentence naming what the proof establishes.

The proof must exist in code, not in prose: `./scripts/check-adr-obligations`
searches outside `adr/`, `docs/`, `work_packages.yaml`, `SPEC.md`, and
`governance/`, and fails once the owning package closes without it.
-->

## Context

At most two paragraphs. Name the force that requires a durable decision, the
authoritative requirements in play, and the conflict an implementation package
cannot settle on its own.

## Decision

1. One testable statement.
2. Another testable statement. Distinguish POC constraints from
   future-compatible extension points, and do not weaken `SPEC.md` silently.

## Standing design tests

Answer both; "not applicable" requires one sentence of reasoning.

- **Interface safety (AGENTS.md boundary 11):** can an application developer or
  agent express an unsafe operation, or silently opt out of a guarantee,
  through any surface this decision adds or changes? If yes, the decision must
  remove that expressibility or escalate for explicit human acceptance.
- **Scale:** does this decision assume co-located authoritative storage,
  single-node memory, or full-state rewrite? Any yes forecloses the
  billion-row tier and must be named as a deliberate, reversible POC
  constraint.

## Checks

- The fixtures, property tests, conformance tests, fuzz targets, crash tests,
  and architecture checks that freeze the decision. Every `obligations[].proof`
  names one of them.
