---
adr: "0213"
title: Pre-Arm Preservation and Linear Queued Fresh-Locator Validation
status: proposed
tier: guarantee
date: 2026-09-07
accepted: null
requires: [ADR-0100, ADR-0102, ADR-0104, ADR-0165, ADR-0197, ADR-0205, ADR-0206, ADR-0208, ADR-0210]
amends: [ADR-0197, ADR-0205, ADR-0206, ADR-0208, ADR-0210]
supersedes: []
requirements: [OUT-001, OUT-002, TXN-042, REC-004, PERF-008, PERF-015, PERF-017, PERF-019, END-008]
packages: [WP-705]
obligations:
  - id: OBL-0213-1
    package: WP-705
    proof: fresh_locator_recognized_pre_arm_control_writes_preserve_uninitialized
    says: Exact recognized pre-arm preserving Immediate writes retain Uninitialized without minting a witness, while every invalid, unknown, uncertain, poisoned, or lost-witness case disables coverage and Disabled remains monotonic.
  - id: OBL-0213-2
    package: WP-705
    proof: fresh_locator_queued_frame_validator_is_linear_exact_and_bounded
    says: The queued command witness validates every ADR-0197 Decision 6 fact from the exact mutations returned by the same composite seal, the sealed frame metadata, and ordered deltas in bounded one-pass work, decoding each physical segment once and refusing all missing, duplicate, extra, sequence-reordered, malformed, substituted, oversized, and uncertain evidence.
  - id: OBL-0213-3
    package: WP-705
    proof: production_grouped_command_arms_fresh_locator_coverage_without_manual_arm
    says: The real bounded-fast-start Group path preserves Uninitialized across its recognized startup controls, arms on its first command, and completes two commands and distinct novel inspections with zero rebuilds and zero fallback scans.
  - id: OBL-0213-4
    package: WP-705
    proof: production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths
    says: Architecture checks freeze pre-arm state handling and the queued validator's exact frame source, one-pass bounds, complete fact inventory, and prohibition on point, history, composite-successor, and repeated manifest reads.
review_triggers:
  - A pre-arm lane would arm, publish, mint or retain a witness, skip the exact closed permit checks, preserve Uninitialized for an unknown or invalid operation, or revive Disabled.
  - Queued evidence would come from anything except the exact mutation slice returned by the same composite seal, its sealed frame metadata, and ordered writer-private deltas, treat physical-mutation position as semantic association, omit canonical manifest ordering or a Decision 6 fact, scan any population, or revisit a command or manifest entry.
  - Queued validation would resolve a composite-successor point, call command authority lookup, reuse the direct helper, retain state beyond validation or any population-sized proof state, exceed the accepted frame and command bounds, or turn error or uncertainty into absence or coverage.
  - The direct or hardened helper, durable bytes, interface, transaction, ordering, acknowledgement, publication visibility, fencing, failure, restart, threshold, or evidence rule would change.
  - WP-705 would use a repair path beyond the exact four authorized here, or ADR-0210 observation would gain control authority or fail to cover the repaired production path.
---
# ADR-0213: Pre-Arm Preservation and Linear Queued Fresh-Locator Validation

## Context

ADR-0197 Decision 11 permits closed preserving Immediate lanes while fresh
locator coverage is armed, but valid startup controls run before the first
command can arm it. The current permit path exits before validating those
controls, after which the commit path disables `Uninitialized`. A later exact
first-command arming attempt therefore sees every accepted arming fact true but
cannot leave `Disabled`. The existing state machine can instead validate the
same closed permit and reconcile it while `Uninitialized`, producing no witness
and retaining the one permitted future arming attempt.

ADR-0197 Decision 6 requires exact queued command evidence. The current queued
check repeatedly filters each segment manifest and resolves locator and command
authority through the growing composite successor for every command. A
controlled 1,217-command run attributes material quadratic work to that seal
edge. The same writer already owns the canonical just-sealed encoded command
frame, its physical mutations, and ordered transient deltas before submission
or publication; they can prove every existing Decision 6 fact without reading
the growing successor or retained history.

## Decision

1. ADR-0197 Decisions 6, 10, and 11; ADR-0205 Decisions 3, 5, 6, 9, and 10;
   ADR-0206 Decisions 1 and 5 through 8; ADR-0208 Decisions 1 and 5 through 8;
   and ADR-0210 Decisions 1, 8, and 9 are amended only as stated here for these
   two repairs. Their implementation and proof may touch exactly:
   `crates/riffdb-storage-redb/src/store.rs`,
   `crates/riffdb-storage-redb/src/store/tests.rs`,
   `crates/riffdb-storage-redb/tests/architecture.rs`, and
   `tests/command_semantics/command_concurrency.rs`. `store.rs` is solely for
   Decisions 2 through 9; `store/tests.rs` solely for their bounded positive
   and negative semantic proofs; `architecture.rs` solely for the Decision 2
   through 12 source proof; and `command_concurrency.rs` solely for the
   Decision 11 and 12 production proof. All other WP-705 authority and purposes
   remain unchanged; a required edit or purpose elsewhere stops for another
   accepted amendment.

2. The pre-arm repair is one separate commit. ADR-0197 Decision 11 is narrowed
   so a recognized `PreservingImmediateClass` is fully assessed even while
   coverage is `Uninitialized`: publication FIFO is empty, expectations are
   closed, expected and actual mutation sets are independently bounded,
   nonempty, and exactly equal, and every table, key, and insert/delete action
   is permitted for that class. Only then may the existing preserving transition reconcile
   the permit. In `Uninitialized` it returns no witness and leaves the state
   `Uninitialized`; it cannot arm, publish, advance, or establish coverage. The
   same access retains its precommit application frontier, allocator value, and
   authority digest for the exact postcommit comparison in Decision 3.

3. An unknown or unclassified operation retains its existing underlying commit
   result while monotonically disabling coverage before visibility. Unclosed,
   missing, late, extra, duplicate, mismatched, oversized, or prohibited
   mutations, a nonempty publication FIFO, borrow or lock failure, poison, I/O
   error, uncertainty, or arithmetic failure retain their existing typed error,
   disablement, and fencing behavior. An armed state with a lost or mismatched
   role is reconciled to `Disabled` before commit visibility. `Disabled` remains
   monotonic. The exact closed class and permit inventories in ADR-0197 Decision
   10 do not expand. Before publishing a valid pre-arm root, its successor must
   have a different root identity, equal application frontier and allocator
   value, and a byte-identical authority digest; mismatch or unreadable successor uses the
   same typed failure, disablement, and fencing as an armed preserving mismatch.

4. The queued-validation repair is a second separate commit. ADR-0197 Decision
   6 is narrowed only in proof mechanism: immediately after the same live
   writer seals the composite successor and before lane submission, private
   successor installation, witness sealing, or publication registration, one
   private validator consumes the exact ordered mutation slice returned by
   that seal, the already sealed journal-frame metadata, and that batch's
   ordered transient deltas. It does not query the composite successor, a point
   resolver, command authority, or history.

5. The validator checks command kind; the sealed frame identity and hash;
   database identity; predecessor and covered application and administration
   frontiers; transition, command, audit, encoded-byte, and stage counts; checked
   predecessor-successor spans; and the exact bounded delta inventory. Command-
   segment deltas and decoded physical command segments must be nonempty and database-bound,
   contiguous, gap-free, nonoverlapping, and jointly cover the declared first,
   last, and command count. Delta/physical agreement is proven during those
   same per-element checks, never by a separate full equality traversal.
   Caller, reconstructed, later, or externally supplied evidence is refused.
   After exact validation, the existing successor stamp and `seal_command`
   retain their root, allocator, frontier, authority-digest, count, checked
   successor, and private-witness checks before submission.

6. Physical-mutation iteration position is not semantic association evidence;
   every decoded manifest retains ADR-0102's strict canonical order. In one
   pass over frame mutations and one pass over each segment, command, and
   manifest entry, fixed ordinal slots and seen bits prove: exactly one
   insert-only `COMMITS` put for
   every segment at its canonical first-sequence key and no other command-frame
   `COMMITS` mutation; exactly one insert-only `IDEMPOTENCY_LOCATORS` put for
   every command at its canonical full-identity key; canonical
   `StoredCommandLocatorV1` bytes naming that exact sequence; and no missing,
   duplicate, extra, replacement, or delete in either authority table.

7. For every command, exactly one idempotency manifest entry must bind its
   canonical exact key, segment first sequence, command ordinal, member ordinal
   zero, and `Command` member. The stored capsule sequence and database,
   environment, tenant, principal, lineage, command ID, and keyed digest must
   derive that same key. Started and terminal audit sequences must be the exact
   two consecutive administration positions and the aggregate audit span must
   agree. Unrelated canonical command mutations remain allowed; they cannot
   satisfy or replace any required fact.

8. Malformed or noncanonical frame, segment, locator, capsule, key, or value
   bytes; any identity-field substitution; wrong kind, database, span, count,
   ordinal, member, sequence, audit position, mutation action, or table; empty,
   over-limit, overflowed, sequence-reordered, gapped, overlapping, missing,
   duplicate, or extra evidence returns no queued witness and monotonically
   disables the shortcut. Codec, I/O, poison, borrow, and arithmetic errors
   retain their existing typed fail-closed behavior and cannot acknowledge a
   shortcut result.

9. Work is `O(frame mutations + deltas + encoded segment bytes + commands +
   manifest entries)`: each physical segment is decoded once and every bounded
   collection element is inspected at most once. Fixed slots and bitsets are
   bounded by the existing 256-transition and 16 MiB frame ceilings; no
   population, retained-history, or growing-successor collection is created.
   Tests cover an exact maximum-256 event-bearing multi-segment Group frame,
   257 and one-over-byte-limit refusal, association independent of physical-
   mutation position within the seal-returned slice, a canonical key-ordered
   manifest whose command ordinals are nonmonotonic, and every Decision 5
   through 8 refusal. Architecture checks reject a separate delta-equality
   pre-pass; structural operation counts, never wall time, prove the bound.

10. The direct and hardened point-read helper remains unchanged and retains its
    original writer transaction, locator reads, command-authority comparison,
    and exact result algebra. Both repairs preserve durable and journal bytes,
    public and test interfaces, command/result semantics, transaction and audit
    ordering, acknowledgement, publication visibility, cancellation, fencing,
    restart, and all ADR-0205 audit-cause and threshold rules.

11. The production proof begins from verified bounded fast start with Dormant
    indexes and zero rebuilds. Its recognized Projection, QueryModule, and
    Consumer startup controls retain `Uninitialized`; the first real Group
    command arms and publishes exact coverage; two commands, two distinct
    post-publication novel inspections, and successful shutdown retain zero
    rebuilds and zero fallback scans. It uses the existing passive ADR-0210
    controller only and does not manually arm or add scheduling authority.

12. No WP-705 runtime evidence is eligible until the pre-arm and queued
    repairs are separate commits and their semantic, negative, architecture,
    direct-helper, grouped-command, clean-start, restart, corruption, and
    bounds checks pass together on the exact evidence source revision.

13. After exact-text acceptance, the separate acceptance commit adds both
    ADR-0210 and ADR-0213 to `WP-705.required_adrs` and regenerates only the
    required ADR status/index metadata. Those governance files are not runtime
    implementation or proof authority under Decision 1.

## Options considered

1. **Ignore pre-arm controls:** rejected; valid startup commits permanently
   disable the sole accepted first-command arming attempt.
2. **Arm before startup controls:** rejected; startup is not an accepted command
   transaction and cannot satisfy the exact-empty live-writer conjunction.
3. **Validate the composite successor per command:** rejected; it is exact but
   repeatedly decodes growing command evidence.
4. **Trust deltas or mutation order:** rejected; neither independently proves
   the physical frame or all identity and manifest facts.
5. **Validate one sealed frame in bounded passes:** chosen; it retains all
   existing checks while removing repeated successor and history work.

## Consequences

- Exact recognized startup controls no longer consume the one arming
  opportunity; invalid or uncertain controls still disable coverage.
- Queued witness validation becomes linear in one already-bounded frame and
  holds only fixed-size ordinal state.
- The second repair duplicates no durable authority: the sealed frame remains
  the physical source and deltas are required to agree with it exactly.
- General pre-arm preservation, new preserving classes, public coverage state,
  new hooks, relaxed frame checks, and pre-gate production evidence claims are not authorized.

## Standing design tests

- **Interface safety:** applications, agents, operators, transports, and
  configuration still cannot observe, request, reset, preserve, or bypass
  coverage. No public, protocol, storage, test-control, or scheduling surface
  changes.
- **Scale:** pre-arm reconciliation uses the existing fixed permit inventories;
  queued validation checks one at-most-16-MiB frame, decodes each segment once,
  and inspects each of at most 256 commands and its bounded evidence once.

## Checks

- `fresh_locator_recognized_pre_arm_control_writes_preserve_uninitialized`
  proves exact pre-arm preservation and all invalid, poison, lost-role, and
  monotonic-disable cases.
- `fresh_locator_queued_frame_validator_is_linear_exact_and_bounded` proves the
  complete D6 fact matrix, mutation-position-independent association, canonical
  nonordinal manifest order, exact maximum event-bearing multi-segment validity,
  exact/one-over bounds, failure atomicity, and absence of repeated reads.
- `production_grouped_command_arms_fresh_locator_coverage_without_manual_arm`
  proves the startup-control sequence and real Group behavior end to end.
- `production_fresh_locator_arming_site_is_reachable_from_both_command_batch_paths`
  freezes the four-file authority, two separate repairs, state transitions,
  one-decode-per-segment routes and bounds, exact fact inventory, forbidden
  reads, and unchanged direct/hardened helper.
