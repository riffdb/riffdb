---
adr: "0274"
title: Shared Immutable Declared Outcome Values
status: accepted
tier: surface
date: 2026-10-06
accepted: "2026-10-14"
acceptance: "maintainer, in session, 2026-10-14: Keep going (ADR-0274 implementation already merged in ad7efb995)"
requires: []
amends: []
supersedes: []
requirements: [OUT-001]
packages: []
obligations: []
review_triggers:
  - A mutable accessor or extraction API for a declared outcome value.
  - A change to outcome equality, canonical bytes, bounds, or redaction.
---
# ADR-0274: Shared Immutable Declared Outcome Values

## Context

The command record graph clones the declared outcome into the stored outcome,
commit record, and retained expected outcome. Its canonical bytes are already
shared, but each clone deep-copies the canonical value tree. This decision
shares that immutable tree as well; it adds no command, wire, or durable format.

The storage-adapter Rust accessor currently has a public `const fn` qualifier.
Borrowing through an `Arc` cannot retain that qualifier on the supported Rust
toolchain. This is an explicit source compatibility change for const callers,
not a claim that the Rust API is unchanged. Ordinary callers and generated
application interfaces retain their signatures and behavior.

## Decision

1. `DeclaredOutcome` MUST retain its canonical value in a private
   `Arc<CanonicalRecord>`. Construction MUST validate and encode the value as
   before; cloning shares immutable ownership without exposing mutation.
2. `value()` MUST return `&CanonicalRecord` as an ordinary, non-const function.
   Storage-adapter const callers must become ordinary functions. The handbook
   and Rustdoc MUST state this source compatibility change.
3. Equality MUST compare outcome identity, canonical value, and canonical bytes
   by value, including independently constructed outcomes. Sharing MUST NOT
   change durable encodings, semantic-byte limits, redaction, replay, or recovery.
4. Service values and other command values remain outside this change. Clone
   timings MUST NOT be described as end-to-end commit throughput evidence.

## Standing design tests

- **Interface safety:** callers receive only an immutable borrowed canonical
  record. They cannot bypass command validation, authorization, org scoping,
  idempotency, or durability. No public or durable fixture bytes change.
- **Scale:** existing canonical value, nesting, and semantic-byte limits remain
  in force. Construction adds one bounded shared allocation and destruction
  decrements reference counts. Empty outcomes and paths without clones may
  cost more; constructor/clone/drop measurements disclose that tradeoff.
  Storage's conservative decoded-memory charge still counts both outcome DTOs.

## Checks

- `cloned_outcomes_retain_exact_values_and_bytes_after_original_drop` proves
  retained nested values, bytes, equality, identity distinctions, charges,
  and redaction after the original outcome is dropped.
- `checked_in_semantic_wire_vectors_are_current` and codec round trips prove
  unchanged persisted outcome bytes and value equality after reconstruction.
- `./scripts/acceptance` and the merge-required `./scripts/ci-all` verify the
  storage API and its runtime, coordinator, service, and recovery consumers.
- Fixture review: no fixture or generated file changes are included.
