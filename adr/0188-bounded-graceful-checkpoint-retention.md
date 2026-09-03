---
adr: 0188
title: Bounded Graceful Checkpoint Retention
status: accepted
tier: guarantee
date: "2026-09-02"
accepted: "2026-09-02"
requires: [ADR-0019, ADR-0061, ADR-0085, ADR-0101, ADR-0104, ADR-0124,
  ADR-0156, ADR-0157]
amends: [ADR-0019, ADR-0085, ADR-0156]
supersedes: []
requirements: [STO-023, REC-001, REC-002, REC-004, PERF-007, PERF-013, PERF-019]
packages: [WP-774]
obligations:
  - id: OBL-0188-1
    package: WP-774
    proof: graceful_close_retains_exact_current_checkpoint_byte_for_byte
    says: After the durable journal barrier, an exact-current validated-prefix checkpoint and its
      companion proof rows remain byte-identical and no checkpoint write transaction occurs.
  - id: OBL-0188-2
    package: WP-774
    proof: graceful_close_leaves_unusable_checkpoint_state_unchanged
    says: Missing, stale, and boundedly ineligible checkpoint state remains absent or byte-identical
      without a population scan while the CLEAN lifecycle transaction remains last.
  - id: OBL-0188-3
    package: WP-774
    proof: graceful_checkpoint_close_crash_matrix_preserves_dirty_fallback
    says: Every barrier, classification, and CLEAN-commit crash or failure edge recovers through
      exact dirty fallback or one complete clean close without authority or allocator advance.
  - id: OBL-0188-4
    package: WP-774
    proof: graceful_checkpoint_receipt_is_closed_and_redacted
    says: Graceful checkpoint handling reports only closed bounded outcomes and never exposes a
      path, identity, frontier, hash, key, value, row count, or unbounded label.
review_triggers:
  - Shutdown would create, refresh, delete, repair, or reinterpret a validated-prefix checkpoint.
  - Checkpoint classification would scan population rows or trust unbounded process-local state.
  - CLEAN could commit before the journal suffix is durably subsumed or any database write could
    follow it.
  - Dirty fallback would skip an exact end, trust unusable checkpoint state, mutate application
    authority, or advance an application or administration allocator.
  - A receipt, error, log, metric, or public surface would expose checkpoint contents or
    population-derived diagnostics.
---
# ADR-0188: Bounded Graceful Checkpoint Retention

## Context

ADR-0019 Amendment 1 and ADR-0085 Amendment 1 still name graceful shutdown as a
`validated_prefix_checkpoint/v1` write point. A changed authoritative head therefore drives proof
construction, entity-head replacement, metadata publication, and an Immediate commit during close.
WP-669 avoids that work only when the retained checkpoint is already exact-current. ADR-0156 later
requires graceful close to remain population-bounded, forbids moving startup validation to
shutdown, and makes the CLEAN lifecycle transaction the final database mutation. The changed-head
checkpoint path cannot satisfy both contracts.

The validated-prefix checkpoint remains useful after dirty termination, but clean startup no
longer depends on it. Shutdown can preserve a previously earned proof without refreshing it: a
dirty next open either verifies the retained prefix and exact suffix under ADR-0019 or ignores it
and performs complete validation. This record freezes that composition without changing a durable
byte, checkpoint verifier, clean-close binding, or acknowledgement boundary.

## Decision

### 1. Graceful shutdown is no longer a checkpoint write point

This record removes graceful shutdown from ADR-0019 Amendment 1 and ADR-0085 Amendment 1's
checkpoint write points. A successful complete startup validation may still write a fresh
checkpoint under their existing zero-finding, atomicity, and failure rules. Retention, migration,
or other already accepted operations may invalidate or delete a checkpoint exactly where their
own ADR says so. Graceful shutdown never creates, refreshes, deletes, repairs, normalizes, or
re-encodes checkpoint metadata or its `validated_prefix_entity_heads` companion table.

After admission closes and every writer drains, the shutdown coordinator first completes the
ADR-0101/0104 barrier: every published journal suffix is validated and durably subsumed by redb,
the published and checkpoint frontiers agree, the active journal extent is valid and empty, and no
writer except the sealed shutdown coordinator remains. A barrier failure stops close before CLEAN;
the lifecycle remains DIRTY and ordinary recovery owns the suffix.

### 2. One bounded classifier may retain bytes but never manufacture proof

After the barrier, storage opens one immutable final redb view and returns exactly one sealed
`GracefulCheckpointDispositionV1`:

- `RetainedExactCurrent`: the process-generation checkpoint identity, canonical checkpoint bytes,
  final application and administration frontiers, retained metadata, watermark, and constant-time
  table cardinalities satisfy the existing WP-669 exact-current predicate;
- `LeftAbsent`: neither checkpoint metadata nor companion proof rows exist;
- `LeftStale`: canonical retained checkpoint state exists but its bounded identity or final-state
  bindings do not satisfy exact-current reuse; or
- `LeftIneligible`: malformed, partial, unsupported, contradictory, exhausted, or otherwise
  boundedly unusable checkpoint state exists.

Classification reads only fixed metadata, engine table cardinalities, and the process-generation
witness already required by WP-669. It never iterates entity, history, index, audit, provenance,
outbox, projection, capability, or other population rows; never decodes population values; and
never hashes a population. Uncertainty or inability to complete these bounded reads is
`ClassificationFailed`, not `LeftIneligible`.

For the four successful dispositions, checkpoint metadata and companion proof rows remain exactly
as observed. `RetainedExactCurrent` means byte-for-byte retention, not an equivalent rewrite.
Absent stays absent; stale and ineligible bytes stay stale or ineligible. No disposition grants
startup authority or changes dirty verification: the existing loader independently verifies,
ignores, or rejects evidence under ADR-0019 and ADR-0085.

### 3. CLEAN remains one final independent transaction

After a successful classification, the coordinator rereads ADR-0156's bounded final roots and
performs the existing ADR-0157 Immediate `DIRTY(n) -> CLEAN(n+1)` transaction. The lifecycle record
does not bind, bless, repair, or require the optional validated-prefix checkpoint. The checkpoint
is deliberately excluded from ADR-0157's bounded-root hash and remains excluded.

The CLEAN commit is the only database transaction after classification and the final mutation of
the process generation. No checkpoint, derived, replication-control, maintenance, telemetry, or
other database write may follow it. Any later accepted feature that attributes the lifecycle
transition must compose its durable mutations inside that same final transaction and may not add
a later checkpoint step.

`ClassificationFailed` prevents a CLEAN attempt. Root-reread, construction, or known pre-commit
failure leaves DIRTY. Commit uncertainty makes no clean claim and admits no later write; recovery
observes either atomic DIRTY or atomic CLEAN. Previously acknowledged work remains durable,
checkpoint bytes remain unchanged, and close returns a typed internal failure or uncertainty.

### 4. Crash states are closed and recoverable

A crash before or during the journal barrier follows ordinary suffix recovery and is dirty. A
crash after the barrier but before CLEAN observes the drained redb state, unchanged checkpoint
bytes, and DIRTY lifecycle. A crash during CLEAN observes exactly the prior DIRTY state or the
complete CLEAN successor. A crash after CLEAN cannot observe another RiffDB mutation because none
is admitted. Missing, stale, or ineligible checkpoint bytes never turn a cleanly closed database
dirty by themselves; they remain irrelevant to clean-certificate startup and are available only
to the independent dirty checkpoint verifier.

On every dirty path, checkpoint verification and complete exact-end fallback remain unchanged.
Ignoring unusable checkpoint evidence never means ignoring an authoritative finding. Repeated
restart creates no command, event, outcome, provenance, audit, outbox, projection, or application
or administration allocator advance.

### 5. Receipt outcomes are finite and redacted

The sealed non-durable shutdown receipt contains only one disposition (`barrier_failed |
retained_exact_current | left_absent | left_stale | left_ineligible | classification_failed`), one
lifecycle outcome (`clean_committed | clean_not_attempted | clean_failed | clean_unknown`), and
bounded saturating stage durations. Invalid combinations refuse construction: barrier or
classification failure pairs only with not-attempted; every other lifecycle result requires one
successful disposition.

Logs, metrics, test receipts, MCP text, CLI output, and public errors may expose only those closed
tags and bounded durations. They never expose database paths or identities, checkpoint bytes,
frontiers, generations, hashes, keys, values, table or row counts, ignore reasons, engine errors,
or unbounded labels. Internal sources remain behind incident IDs.

## Options considered

1. **Retain without refresh:** selected; shutdown stays bounded and dirty startup still verifies
   the existing proof or performs the complete path.
2. **Keep changed-head checkpoint rebuilding:** rejected because it performs population work before
   CLEAN and contradicts ADR-0156's bounded-close rule.
3. **Delete unusable checkpoint state:** rejected because deletion is an unnecessary mutation and
   could erase useful dirty-prefix evidence without earning a replacement proof.

## Consequences

- Clean close becomes independent of database population and checkpoint write success.
- A later dirty restart may validate a longer suffix or fall back completely; this is the bounded
  shutdown tradeoff, not a weakened recovery guarantee.
- No durable format, registry, checkpoint, lifecycle, journal, or public interface identity changes.
- Opportunistic runtime checkpoint creation remains deferred.

## Standing design tests

- **Interface safety:** No application, agent, operator option, configuration, or transport can
  select a disposition, request checkpoint refresh, force CLEAN, or suppress dirty fallback.
- **Scale:** Classification uses fixed metadata and constant-time engine cardinalities. Graceful
  close performs no population walk, proof construction, or checkpoint write.

## Checks

- The four obligation proofs, storage recovery matrix, process graph ordering tests, and real
  daemon restart tests freeze byte retention, no-scan behavior, crash states, and redaction.
- Durable fixtures and version topology remain byte-identical; `ci-all` proves no hidden format
  or generated-surface change.
