# WP-813: Write proof review checkpoint

Status: preparation only; ADR-0262 remains proposed. No semantic implementation
or merge is authorized by this note. The package is open and has no completion
date. The implementation branch uses a dedicated worktree, to be removed only
after its changes are merged to main and its working tree is clean.

## Finding and evidence

The accepted binding-wide rule lets `PutState` replace a payload while merely
incrementing revision. The characterization test
`revision_increment_alone_allows_serialized_payload_replacement` compiles the
existing initialized-state fixture and confirms explain reports no unguarded
writes. Starting with revision zero, it evaluates two distinct requests in an
explicit serialized schedule. The second sees the first replacement post-image.
Both yield `Written` mutation candidates; revision reaches two and the second
payload replaces the first.

This is deterministic compiler/runtime evidence. It does not exercise admission,
the commit coordinator, persisted idempotency, provenance, or crash recovery.
Those integration assertions belong to the implementation acceptance gate.

## Decision for review

[ADR-0262](../../adr/0262-separate-write-dependencies-from-precondition-proofs.md)
contains the exact proposed rules. In particular, named business associations
remain author claims about relevance; arithmetic evidence is not caller
freshness; revision fences depend on a consistently maintained revision
protocol. Acceptance must include these explicitly stated limits.

WP-809, the hard dependency, is complete and its implementation commit is an
ancestor of this worktree's main baseline. WP-813 declares the syntax, compiler,
IR, runtime, diagnostics, CLI, contract, fixture, and documentation scope needed
for implementation. The next implementation step after acceptance is a failing
compiler regression rejecting replacement set authorized solely by a revision
increment. Keep historical behavior in compatibility evidence when migrating
the current characterization fixture.

## PR note for this checkpoint

- Package: WP-813, preparation checkpoint only.
- Tier: guarantee, matching the proposed semantic decision.
- Behavior added or changed: deterministic characterization of existing behavior;
  proposed ADR, package registration, and generated ADR index entry.
- Checks run: the focused runtime characterization passes; scoped acceptance
  passed all 15 steps, including 56 runtime tests, clippy, formatting, scope,
  governance, and the complete handbook check. The full merge battery remains
  due after semantic implementation; this checkpoint is not package completion.
- Compatibility: no public runtime or compiler behavior changes in this checkpoint.
- Handbook: no public handbook changes yet because the proposed syntax and
  classifications are unavailable; implementation must update the reachable
  language, cookbook, diagnostic, explain, and compatibility pages together.
- Hazards and follow-ups: obtain exact-text human ADR acceptance before semantic
  changes; add per-write evidence and versioned validation, migrate contracts,
  prove service/coordinator semantics, and run full merge checks before closure.
