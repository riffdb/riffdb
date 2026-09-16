---
adr: "0233"
title: Typed Boolean Operands In Row Policy Evaluation
status: accepted
tier: guarantee
date: 2026-09-16
accepted: 2026-09-16
acceptance: 'maintainer, in session, 2026-09-16: "Accept ADR-0233 as written"'
requires: [ADR-0111]
amends: []
supersedes: []
requirements: [RAP-001, RAP-002, RAP-009]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers: []
---
# ADR-0233: Typed Boolean Operands In Row Policy Evaluation

## Context

ADR-0231's process tests exposed a pre-existing availability defect in the shared
row-policy evaluator. A compiled `allow create when true` is denied. Operands
produce `NodeValue::Canonical(CanonicalValue::Bool(...))`, but `boolean_node`
accepts only the separate `NodeValue::Boolean` produced by comparisons. Boolean
constants, fields and facts therefore fail when used directly as predicates or
inside logical operators, despite passing the compiler's Boolean type checks.
An equivalent field self-comparison succeeds in the same real command fixture.

This can deny every command or query using an otherwise valid unconditional
policy, so it is a high-severity repair permitted by D-001. The change restores
ADR-0111's typed Boolean semantics without adding a language construct. AGENTS.md
requires human review because a policy previously denied by this defect can now
allow an operation. Acceptance is required before changing the evaluator.

## Decision

1. At a Boolean-use site, accept either the Boolean result of an evaluated
   comparison/logical expression or a canonical Boolean operand already checked
   against its compiled value type. Return the exact Boolean value in both cases.
   The implementation adds the `Canonical(CanonicalValue::Bool(value))` arm to
   `boolean_node`; it does not coerce other canonical values.
2. Keep operand type validation, bounded expression traversal, complete required
   relationship evidence and existing invalid-plan/row refusal. Missing fields,
   missing or mistyped principal facts, non-Boolean values and invalid node
   references must not become false, true or an implicit allow.
3. Preserve both representations. Equality/inequality and membership continue to
   consume canonical operands; accepting Boolean operands at a Boolean-use site
   must not make canonical Boolean equality invalid or alter null semantics.
4. The common evaluator continues to serve all existing policy enforcement
   paths. Capability revision, lifetime, scope, role/operation binding, current
   and successor checks, read filtering and final release checks remain intact.
   No request option or privileged bypass is added.
5. No grammar, IR, durable encoding, capability, provider, cursor or public error
   identity changes. Existing compiled policies receive their declared Boolean
   semantics after upgrade. Operators must be told that literal/direct Boolean
   policies which previously failed closed can now allow their intended rows.

## Standing design tests

- **Interface safety:** only the existing compiled policy and current authorized
  principal decide admission; application values cannot bypass type or authority
  checks. No general truthiness, missing-value default or unchecked shortcut.
- **Scale:** constant-time discrimination of an already bounded evaluated value;
  no new allocation, state, lookup, recursion or scan limit.
- **Recovery:** no persisted format changes; policy is re-evaluated at the same
  existing authoritative and release safe points after restart.

## Checks

- Differential truth tables for Boolean constants, row fields and principal
  facts at the root and under NOT/AND/OR, including Boolean equality/membership.
- Exercise true and false across read/create/delete and both rows of an update;
  a false predecessor or successor must still deny the update.
- Reject missing/mistyped inputs and absent relationship proof without release.
- Restore literal-true create/update/delete clauses in the real ADR-0231 policy
  fixture; confirm committed provenance/idempotency and exact protected query
  membership through allow/deny transitions, follower replay and restart.
- Run policy/conformance/command tests and the complete touched-path acceptance;
  document the correction in the row-policy authoring handbook.
