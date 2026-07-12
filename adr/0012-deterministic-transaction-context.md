# ADR-0012: Deterministic Transaction Context

- **Status:** Proposed
- **Direction approved:** 2026-07-12
- **Exact text accepted:** No
- **Decision deadline:** Before WP-080 implementation

The human architecture review approved this direction. This record remains
Proposed until its exact text is reviewed and accepted.

## Context

Runtime output must be reproducible from declared inputs. `tx.time` is required by
commands but cannot come from an OS clock during execution. Before the 2026-07-12
governance reconciliation, WP-080 wording about deterministic randomness also
conflicted with normative `TXN-011`, which forbids POC command randomness.

## Proposed Decision

POC commands have no randomness instruction, value, seed, or capability.
Randomness used for capability tokens is outside the deterministic command
runtime and never exposed to contract execution. Recorded deterministic command
randomness is deferred to an MVP ADR.

Before evaluation, the commit coordinator durably creates or resumes the
idempotency admission. It records the coordinator-observed logical `tx.time`,
canonical input hash, request ID, contract version, and exact plan hash. Runtime
receives these immutable values plus a bounded materialized snapshot as a
value-only `TransactionContext`; it does not read clocks, randomness, filesystem,
network, environment, process-global mutable state, locale, or nondeterministic
collection order.

A retry of the same pending admission reuses its recorded time and plan. A
pending admission has no commit sequence. Runtime evaluation is synchronous and
contains no `.await` after mutable capabilities are acquired. Test seeds may
drive generation and scheduling in the harness but are not visible to command
semantics.

## Options Considered

1. **No POC command randomness plus recorded admitted time:** Approved and matches
   normative requirements.
2. **Seeded deterministic command randomness:** Reproducible but violates the POC
   requirement and expands IR/value/record semantics.
3. **Derive time from request ID:** Avoids a record but gives identifiers an
   unintended clock meaning and complicates correction.
4. **Read wall clock during evaluation:** Hidden nondeterminism and retry drift.

## Consequences

- WP-080 deliverables/tests must remove any implication that commands receive a
  deterministic random seed.
- Admission persistence precedes evaluation and is part of crash recovery.
- Runtime can be compared byte-for-byte with a reference model.
- Future randomness requires a new accepted ADR, recorded values, IR support, and
  compatibility fixtures.

## Compatibility

Transaction-context fields, logical time encoding/precision, admission record,
plan identity, and any future context addition are semantic/durable boundaries.

## Security

Runtime cannot use ambient authority or entropy. Context fields are bounded and
authorization-resolved. Test hooks cannot compile into or mutate production
global state.

## Testing

Same input/snapshot/context equality, randomized property histories with harness
seeds hidden from runtime, forbidden-dependency architecture checks, retry/crash
tests preserving `tx.time` and plan hash, collection-order permutation tests, and
Miri/Clippy-style policy checks where useful.

## Requirements and Work Packages

- **Requirements:** `TXN-010` through `TXN-013`, `TXN-001`, `TXN-030`, `SYS-003`
- **Defines or blocks:** `WP-060`, `WP-080`, `WP-100`
- **Final evidence:** `WP-190`, `WP-200`

## Decision Deadline

Exact acceptance is required before WP-080 implements transaction context or
runtime instructions. WP-060 may reserve only the admission fields already
accepted through ADR-0005/0012.
