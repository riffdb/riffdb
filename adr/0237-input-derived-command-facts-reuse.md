---
adr: "0237"
title: Input-Derived Command Facts Reuse
status: proposed
tier: guarantee
date: 2026-09-19
accepted: null
acceptance: null
requires: [ADR-0107, ADR-0126, ADR-0183]
amends:
  - ADR-0107 only to the extent described below: transaction-current command
    validation derives the input proof once per attempt and proves its bind,
    rather than re-deriving it at each stage.
supersedes: []
requirements: []
packages: []
obligations:
  - id: OBL-0237-1
    package: null
    proof: command_validation_seals_one_exact_attempt_before_index_or_record_authority
    says: Transaction-current validation proves the supplied input proof is
      bound to the exact plan and normalized input in hand before reading any
      position, key, or range out of it.
  - id: OBL-0237-2
    package: null
    proof: transaction_current_validation_refuses_a_foreign_input_proof
    says: An input proof derived from a different command is refused by
      transaction-current validation rather than acted on.
review_triggers:
  - A stage stops proving the bind before reading positions, keys, or ranges
    out of an input proof it did not itself derive.
  - The input proof becomes reachable across a crate boundary, or from a
    caller outside the sealed admission chain.
  - InputDerivedCommandFacts gains a way to be cloned, mutated, or constructed
    without deriving it from a plan and a normalized input.
---
# ADR-0237: Input-Derived Command Facts Reuse

## Context

`derive_input_command_facts` is a pure function of a command plan and its
normalized input. It re-validates the input shape, re-evaluates the partition
expression, every conflict-key component, every binding key and every
root-validation key through the shared expression evaluator, and deep-clones
the normalized input on each call. Instrumenting a running server on the
write_only benchmark showed one command driving exactly eight derivations of
the same facts, from eight distinct call sites across four crates. Command
preparation already derived the proof before admission discarded it.

ADR-0107 requires that transaction-current validation derive reverse-index
prefixes, cascade bounds, and collection slots from the sealed plan and the
exact normalized input, so that callers cannot submit binding indices, keys,
or range counts and thereby acquire storage authority. That requirement was
satisfied by re-deriving the proof inside each stage, which made the bind
between proof and command hold by construction. Reusing one derivation removes
that construction, so the bind has to become a check or the guarantee is lost.

## Decision

1. The proof travels from command preparation, through admission, into the
   pending attempt, and every later stage inside `riffdb-commit` borrows that
   one value. No stage in `riffdb-commit` derives its own.
2. Any stage that reads a position, key, or range out of a proof it did not
   itself derive first proves `matches_command`, which compares the plan hash
   and the complete normalized input, and fails closed as an integrity error
   otherwise. This is an explicit early guard rather than the sole
   enforcement: the identity and position comparisons against the evaluated
   request already reject a mismatched proof, and were confirmed to do so with
   the guard removed. The guard makes the requirement legible and fails fast
   and cheaply, before any key or range is read.
3. ADR-0107's guarantee is unchanged in substance: a caller still cannot
   submit binding indices, keys, or range counts, because the only proof any
   stage will act on is one that provably belongs to the command in hand.
4. The reuse stays inside `riffdb-commit` and the sealed admission chain. The
   catalog snapshot-materialization entry and the runtime execution entry keep
   deriving their own proofs; threading the proof across those boundaries was
   measured at about one percent and is not worth relaxing two further
   reviewed contracts.
5. `InputDerivedCommandFacts` remains move-only and non-cloneable. Reuse is by
   shared borrow of a single value, so exactly one proof exists per command,
   as before.

## Standing design tests

- **Interface safety (AGENTS.md boundary 11):** no. The proof is crate-private
  to the sealed admission chain and cannot be constructed, cloned, or supplied
  by an application developer or agent. The one new way a proof can reach a
  stage is from another stage of the same attempt, and that path is checked by
  `matches_command` before use. No surface gains a way to express an unsafe
  operation or opt out of a guarantee.
- **Scale:** no. The change removes work and allocations from a per-command
  path and assumes nothing about co-located storage, single-node memory, or
  full-state rewrite. It reduces per-command memory traffic rather than adding
  retained state.

## Consequences

Eight derivations per command become three, all outside `riffdb-commit`. On the
C3D bench host this contributed to a measured write-path gain of roughly two to
four percent, alongside the durable-record framing changes recorded in the same
programme. The writer census keeps its input-facts stage, which now reports the
single derivation performed at admission rather than a per-stage sum.

The cost is that a bind previously held by construction is now held by
comparison. That is less of a change than it first appears: the downstream
identity and position checks already compared facts-derived values against the
evaluated request, and reject a foreign proof on their own. What is new is an
explicit guard stating the requirement at the point the proof arrives, which
costs a plan-hash comparison and one record equality and allocates nothing. It
is the same guard command preparation already applied to the proof it
received, so the pattern is not new to this crate.

## Options considered

Re-deriving in every stage, as before, is correct and needs no record, but
pays seven redundant derivations per command for a bind that a single
comparison establishes.

Threading the proof across the catalog and runtime boundaries as well was
implemented and measured. It reached one derivation per command but gained
about one percent, with the highest concurrency level flat inside run-to-run
noise, in exchange for relaxing two further reviewed contracts and changing a
public signature. It was reverted.

Making `InputDerivedCommandFacts` cloneable would avoid threading entirely.
It was rejected: the type is deliberately non-cloneable, with a compile-fail
test pinning that, so that exactly one proof exists per command.

## Checks

- `command_validation_seals_one_exact_attempt_before_index_or_record_authority`
  pins the ordered mechanisms in transaction-current validation, including the
  `matches_command` bind, and fails if that line is removed from the source.
- `transaction_current_validation_refuses_a_foreign_input_proof` drives a proof
  derived from different input through validation and requires an integrity
  error. It pins the refusal rather than one mechanism: the refusal was
  confirmed to survive removing the `matches_command` guard, because the
  identity and position comparisons reject the proof independently.
- The compile-fail doctest on `InputDerivedCommandFacts` keeps the proof
  non-cloneable.
