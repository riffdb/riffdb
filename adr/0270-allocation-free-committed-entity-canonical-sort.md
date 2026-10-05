---
adr: "0270"
title: Allocation-Free Committed Entity Canonical Sort
status: proposed
tier: guarantee
date: 2026-10-05
accepted: null
acceptance: null
requires: []
amends: []
supersedes: []
requirements: [BLK-066, BLK-069]
packages: []
# One entry per deferred obligation. `proof` is a test function name or a
# scripts/<name>; ./scripts/check-adr-obligations requires it to exist as a
# definition once the owning package is complete.
obligations:
  - id: canonical-sort-equivalence
    text: >-
      The allocation-free comparator MUST order committed entity mutations
      identically to the canonical encoded sort key
      `[0x01, entity_type_id_be, key_len_be, key_bytes]` for every input,
      including equal type ids, differing key lengths, and byte-equal prefixes.
    proof: canonical_entity_sort_matches_encoded_key_order
review_triggers:
  - A change to the committed-entity canonical order, the entity sort key
    encoding, or the atomic command record set assembly.
---
# ADR-0270: Allocation-Free Committed Entity Canonical Sort

## Context

`sort_committed_entities` in `riffdb-commit` establishes the canonical order of
committed entity mutations inside the atomic command record set that the commit
coordinator assembles, hashes, and persists for one command. Any change to code
under `crates/riffdb-commit/src/**` is governance guarantee tier because that
crate owns sequence assignment and authoritative record assembly; even a
performance-only edit is reviewed as if it could alter commit semantics.

The current sort calls `sort_unstable_by_key` with a closure that rebuilds the
full encoded sort key as a fresh heap `Vec` on every key extraction. The sort
calls the key function many times per element, so a multi-entity command (up to
the 4,096-mutation `BLK` ceiling) pays O(n log n) small allocations purely to
order rows whose order is already fully determined by two in-struct fields. The
decision is whether the ordering may be computed without that per-call
allocation while keeping the committed order, and therefore record-set bytes,
hashes, idempotent replay, and crash-recovery visibility, byte-for-byte
unchanged.

## Decision

1. `sort_committed_entities` MUST order mutations by `entity_type_id` ascending,
   then by entity-key length ascending, then by entity-key bytes
   lexicographically, comparing those fields in place with no per-comparison
   heap allocation.
2. This order MUST equal, for every input, the lexicographic order of the
   canonical encoded key `[0x01, entity_type_id_be(4), key_len_be(4),
   key_bytes]` the previous implementation produced. The leading tag is a shared
   constant; `entity_type_id` is an unsigned integer whose big-endian byte order
   equals numeric order; the length-prefixed key compares by length then bytes.
   No other field participates in the order.
3. The change is performance-only. The committed record set, its canonical
   bytes, every derived hash, the two durable transitions, idempotent replay,
   and complete-or-absent redb visibility MUST remain exactly as before. No
   public surface, storage key, durable envelope, protobuf field, or IR format
   changes.

## Options considered

1. **Keep `sort_unstable_by_key` with the encoded-key closure.** Correct but
   allocates a `Vec` per key extraction, scaling with command cardinality on the
   authoritative commit path. Rejected: avoidable allocation on the hottest
   write path.
2. **Precompute each encoded key once into a parallel vector, then sort indices.**
   Removes per-comparison allocation but still allocates one buffer per entity
   and adds an indirection pass. Rejected: the order depends only on two fields
   already present in each struct, so no materialization is needed.
3. **Compare the two fields in place (chosen).** Zero per-comparison allocation,
   identical order, smaller code. Chosen.

## Consequences

- Positive: eliminates O(n log n) small heap allocations per multi-entity
  command on the commit coordinator path; measured ~6x faster comparator on an
  8 vCPU AMD EPYC 7B12 (n=1000: 477us -> 81us; n=8192: 5.50ms -> 0.94ms).
- Constraint: the comparator now encodes the canonical order's field structure
  directly, so any future change to the encoded sort key MUST update both the
  comparator and the equivalence proof together; the review trigger enforces
  this.
- Deferred: nothing. This decision does not change any order, byte, hash, or
  bound; it only changes how the unchanged order is computed.

## Standing design tests

- **Interface safety:** no public or application-facing surface changes. The
  committed order is an internal durable property; an application developer or
  agent cannot express or observe a different order, and cannot opt out of the
  canonical ordering guarantee that hashing and replay depend on.
- **Scale:** the comparator is allocation-free and O(key length) per comparison;
  total work stays O(n log n) comparisons within the existing 4,096-mutation
  `BLK` ceiling, with no per-element heap growth as cardinality rises.

## Checks

- `canonical_entity_sort_matches_encoded_key_order` (unit test in
  `riffdb-commit`) proves the comparator order equals the canonical encoded-key
  order across equal type ids, differing key lengths, and byte-equal prefixes.
- The existing `riffdb-commit` and bulk-command acceptance suites
  (`thousand_element_command_plus_root_commits_and_replays_atomically_through_redb`
  and the `command_concurrency` / `bulk_command_recovery` tests) continue to
  prove atomic commit, idempotent replay, and crash recovery over multi-entity
  record sets unchanged.
