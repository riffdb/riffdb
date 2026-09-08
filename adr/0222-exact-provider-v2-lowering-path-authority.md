---
adr: "0222"
title: Exact Provider V2 Lowering Path Authority
status: proposed
tier: guarantee
date: 2026-09-08
accepted: null
requires: [ADR-0219]
amends: [ADR-0219]
supersedes: []
requirements: [GOV-001, GOV-002, GOV-003, OQ-017, OQ-024, OQ-025, OQ-026]
packages: [WP-757]
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations: []
review_triggers:
  - A production path other than the exact two added to ADR-0219 Decision 9 would be inferred from this record.
  - ExactPredicateProgramV1 would be relabeled, aliased, or retained as a standalone provider selector or runtime provider.
  - Existing exact-predicate compiler or module logic would be copied or moved instead of changed at its current owner.
  - PresentOnlyV1 would be emitted without the complete normalized-predicate and installed-lineage proof required by ADR-0219.
  - V5 bytes, exact-provider semantics, a public surface, storage authority, or transaction behavior would change.
---
# ADR-0222: Exact Provider V2 Lowering Path Authority

## Context

ADR-0219 Decision 9 authorizes the query compiler's legacy exact-text owner and
the query module's legacy exact-text container, but omits the two existing
owners that implement and seal exact-predicate and independent-order families.
The compiler owner constructs both `ExactPredicateProgramV1` and
`ExactPredicateProgramV2`; the module owner types, retains, and canonically
encodes the nonnullable exact-predicate program. Those files cannot carry a
genuine V2 program without production edits.

Working only in the existing list would require copying compiler and module
logic into unrelated files, retaining V1 under a V5 descriptor, or routing
through a different plan variant and editing its dispatcher. Each choice would
violate ADR-0219's ownership, no-alias, or exact-semantics constraints. The
implementation must stop until the two exact owners are explicitly authorized.

## Decision

1. ADR-0219 Decision 9 is amended only to add these production paths:
   `crates/riffdb-query-compiler/src/exact_predicate.rs` and
   `crates/riffdb-query-module/src/exact_predicate_result_set.rs`. The original
   eight paths remain exact. This record authorizes no other production path.

2. The compiler path may change the existing exact-predicate and
   independent-order lowering so every such family produces a genuine
   `ExactPredicateProgramV2`. A nonnullable order term uses `PresentOnlyV1`
   only when the compiler proves that the complete normalized predicate and
   installed lineage admit neither missing nor null for that term. Existing
   nullable families retain their exact `NullsFirstV1` or `NullsLastV1`
   placement.

3. The module path may change the existing sealed nonnullable exact-predicate
   container, its program accessor, and its canonical encoding only as needed
   to carry that exact `ExactPredicateProgramV2` from compiler output through
   the immutable plan identity. It must not reinterpret V1 bytes, attach a V5
   descriptor to a V1 program, or create a compatibility alias.

4. `ExactPredicateProgramV1` remains only the nested semantic base required by
   `ExactPredicateProgramV2` and canonical V5 bytes. It is not a standalone
   provider selector, runtime provider, module payload, descriptor owner, or
   compatibility path. The implementation must delete or make unreachable any
   public V1 descriptor selection rather than relabeling it as layout 5.

5. Existing lowering and module logic stay in their present owners. WP-757
   must not duplicate or move them into an already authorized file to avoid
   this path boundary. In particular,
   `crates/riffdb-query-module/src/lib.rs` remains unauthorized, as does every
   other path not named by ADR-0219 Decision 9 plus Decision 1 above.

6. This amendment changes no predicate truth table, comparison profile,
   null placement, count, ordinal, policy, epoch, readiness, retention,
   refusal, recovery, V5 checkpoint byte, provider algorithm, query grammar,
   public interface, application or agent authority, storage key or format,
   transaction ordering, publication, or acknowledgement semantic. It adds no
   dependency and authorizes no migration or fallback.

7. OBL-0219-1, including its exact proof name
   `epoch_two_exact_provider_state_is_sole_v5_and_plan_rotations_are_exact`,
   remains unchanged. That proof must distinguish genuine V2/V5 state from a
   V1 relabel and preserve ADR-0219's independently reviewed artifact and hash
   rotations.

8. This is a proposal-only change. Until exact human acceptance, it changes
   only this record and the generated ADR index and grants WP-757 no new path
   or implementation authority.

## Options considered

1. Copying the V2 lowering into `src/lib.rs` and the sealed program into
   `exact_text_result_set.rs` is rejected because it duplicates semantic
   owners and invites divergent canonical bytes.
2. Relabeling the V1 program or its descriptor as V5 is rejected because it
   would preserve a predecessor selector while claiming sole-current V2/V5.
3. Routing every family through another plan variant is rejected because it
   changes dispatch and requires the expressly unauthorized module `src/lib.rs`.
4. Adding the two exact owner paths is chosen because it is the smallest
   authority that permits genuine V2 lowering and carriage without changing
   behavior outside ADR-0219.

## Consequences

- WP-757 can replace the predecessor program at its true compiler and module
  owners instead of creating an alias or duplicate.
- Review must verify the two new paths contain only V2 lowering and sealed
  carriage changes and that all original semantic and byte guarantees remain.
- Runtime retirement, generated artifacts, topology, locks, cursors, fixtures,
  and final hashes remain governed by ADR-0219 and are not changed here.

## Standing design tests

- **Interface safety:** applications and agents still cannot select a program
  version, provider identity, null-placement fallback, decoder, or migration;
  the existing finite compiled operation remains the only public expression.
- **Scale:** candidate, input, output, state, work, ordinal, page, retention,
  and diagnostic bounds remain exact; V2 carriage adds no scan, materialize-all
  path, per-row descriptor work, or unbounded collection.

## Checks

- `epoch_two_exact_provider_state_is_sole_v5_and_plan_rotations_are_exact`
  proves genuine V2 lowering for exact-text, filtered, predicate,
  independent-order, and nullable families, `PresentOnlyV1` proof posture,
  structural V1-through-V4 absence/refusal, exact V5 bytes and semantics, and
  the independently reviewed artifact and hash rotations required by ADR-0219.
- WP-757 focused compiler/module semantic and canonical-byte tests, workspace
  check and clippy, requirement and ADR-obligation checks, allowed-path checks,
  and `ci-all` preserve the unchanged guarantees.
