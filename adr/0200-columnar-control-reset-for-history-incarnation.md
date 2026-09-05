---
adr: "0200"
title: Columnar Control Reset for History Incarnation
status: proposed
tier: guarantee
date: 2026-09-05
accepted: null
requires: [ADR-0072, ADR-0160, ADR-0190, ADR-0192]
amends: [ADR-0192]
supersedes: []
requirements: [REC-001, PRJ-004, PRJ-006, PRJ-008, PRJ-009, PRJ-010]
packages: [WP-781, WP-711]
obligations:
  - id: OBL-0200-1
    package: WP-711
    proof: stale_history_incarnation_resets_columnar_control_before_serving
    says: Startup never serves or retains a fence from a stale-incarnation columnar pointer and rebuilds from one fresh unprepared V1 candidate under the current authoritative incarnation.
  - id: OBL-0200-2
    package: WP-711
    proof: columnar_history_reset_uses_transaction_current_incarnation
    says: The reset reads the authoritative incarnation inside its own transaction, accepts no caller-supplied replacement identity, and changes only one exact stale control.
  - id: OBL-0200-3
    package: WP-711
    proof: columnar_history_reset_unknown_commit_rereads_exact_control
    says: Applied, mismatch, storage failure, and unknown commit all resolve by exact durable reread while serving and retention remain closed.
  - id: OBL-0200-4
    package: WP-711
    proof: columnar_history_reset_retires_exact_colliding_candidate_paths
    says: Every durable-shape Unprepared V1 candidate passes unconditional preconstruction path recycling, so reset/spec-reconciliation and repeated construction crashes reuse no artifact and touch only its two exact paths through bounded reference-safe cleanup and crash-safe rename.
  - id: OBL-0200-5
    package: WP-711
    proof: columnar_history_reset_precedes_spec_retarget
    says: Stale-incarnation reset completes before ordinary initial-candidate retargeting, with every runtime and retention gate closed through both durable rereads.
review_triggers:
  - A stale pointer could serve, contribute a retention frontier, or be relabelled under the current incarnation.
  - A caller could choose the replacement incarnation, reset a current or mixed-incarnation control, or change authoritative application state.
  - Collision handling would scan a parent directory, recurse without frozen bounds, reuse an artifact or same-incarnation generation, delete without a reference proof, touch a noncandidate/quarantine path, or open a runtime or retention gate early.
  - A durable encoding, restore operation, application or administration sequence, acknowledgement rule, or public surface would change.
---
# ADR-0200: Columnar Control Reset for History Incarnation

## Context

ADR-0160 requires stale-incarnation columnar state to fail closed and enter the
existing typed rebuild lifecycle. ADR-0192 then closed the durable control
operations, but every operation that allocates a replacement preserves the
selected pointer's history incarnation. After destructive restore advances the
authoritative ADR-0072 incarnation, a restored tag-67 control can therefore be
recognized as stale but cannot lawfully reach a current-incarnation candidate.

Treating the old V1 manifest as current is unsafe because its frozen bytes do
not carry an incarnation. Deleting the control outside its repository would
bypass its CAS and retention-fence role. The missing authority is one exact
storage-internal reset, not a new format or a general repair callback.

## Decision

1. **Stale state closes before use.** During startup reconciliation, before
   query readiness, columnar workers, writers, or retention pruning open, the
   server compares every present Published, Candidate, and Predecessor pointer
   with the authoritative transaction-current history incarnation. Any stale
   pointer is ineligible for serving, process-local view installation, or a
   retention frontier. Mixed pointer incarnations or a zero/invalid identity
   are corruption and are not reset automatically.
2. **One closed transition resets one stale control.** ADR-0192's
   `ColumnarProjectionControlRepository` gains only
   `ResetForCurrentHistoryIncarnation`. It consumes the complete expected
   control and accepts no replacement incarnation, callback, transaction,
   table, key, path, or digest from its caller. Inside one Immediate storage
   transaction the implementation rereads the authoritative
   `history_incarnation/v1` metadata and the exact control row. It applies only
   when the row equals the expected control, every present pointer has one
   identical nonzero incarnation, and that incarnation is strictly less than
   current. Equal, greater, zero, and mixed incarnations refuse before mutation.
3. **The replacement shape is exact.** The transition preserves source,
   target definition fingerprint, target spec hash, and replay limits. It uses
   checked `highest_generation + 1`, clears Published, Predecessor, failure,
   and every old Candidate, and installs exactly one Unprepared V1 Candidate
   at BeforeFirst under the transaction-current incarnation. Lifecycle becomes
   Building. For this reset only, a tag-67 columnar pointer and artifact use the
   complete `(history_incarnation, generation)` identity: a numeric generation
   rewound by restore may repeat only after the authoritative incarnation has
   strictly increased, and never within one incarnation. This exception changes
   no aggregate, tag-65, or other `ProjectionGeneration` allocation semantics.
   Exhaustion, absent pointers, equal or future incarnation, mixed incarnations,
   malformed control, or unreadable current metadata refuses before mutation.
   No application or administration sequence is assigned.
4. **Resolution is reread-only.** Applied, expected-control mismatch, storage
   failure, and unknown commit never authorize serving from the attempted
   replacement. With capture, serving, worker publication, and pruning gates
   closed, the caller rereads the complete durable control. It proceeds only
   from either the exact fresh current-incarnation Building shape or a later
   independently valid current-incarnation state; otherwise the source remains
   unavailable. Repeated restart is idempotent and cannot allocate again after
   a successful reset.
5. **Specification reconciliation precedes construction.** After the reset
   outcome is durably resolved, startup runs ordinary checked-specification
   reconciliation. If target hashes drifted, the existing
   `RetargetInitialCandidate` transition consumes the fresh control before any
   path retirement or runtime construction. The intermediate reset Candidate
   is never constructed; its possible pre-restore directories remain unselected
   cleanup material. Capture, serving, worker publication, columnar writers,
   and pruning remain closed through both transitions and durable rereads.
6. **Exact path collisions are retired before construction.** On every startup,
   after incarnation and checked-specification reconciliation, the durable shape
   of every final Unprepared V1 Candidate unconditionally enters this handler
   before construction. Selection depends only on that exact durable shape, not
   an in-memory reset marker or the presence of quarantine; an Unprepared
   Candidate has no artifact witness, so files at its paths are discardable and
   never adoptable. A crash after reset but before the first rename therefore
   repeats specification reconciliation and this handler. For each of its exact
   ADR-0190 final-directory and `.tmp` paths `P`, quarantine `Q` is
   exactly `<P-name>.retired-before-history-` followed by the transaction-current
   incarnation as sixteen lowercase hexadecimal digits. Under the existing
   exclusive database startup ownership and with no captured view, the handler:
   first boundedly removes a present `Q` only after proving no control or view
   references it and syncing the parent; then atomically renames a present
   expected-directory-kind `P` to absent `Q` without replacement and syncs the
   parent. A crash at any remove, rename, or sync edge repeats this exact state
   machine. Thus a fresh construction crash that recreates `P` while `Q` exists
   recycles `Q`, retires `P`, and can rebuild again rather than wedging.

   Cleanup walks only those two exact roots, rejects symlinks, special files,
   escapes, and material beyond the existing ADR-0160/0190 generation ceilings,
   and performs no parent-directory scan or unbounded recursion. Kind, bound,
   reference-proof, remove, rename, sync, or unknown-outcome failure keeps every
   gate closed. The resulting absent `P` paths alone authorize fresh construction;
   no artifact is adopted or selected from its bytes. Stale-plus-spec-drift
   retires only the retarget-born Candidate's pair, so one reset handles at most
   two source roots and their two deterministic quarantine roots.
7. **Rebuild and publication remain ordinary.** After the final path retirement
   is durably resolved, the resulting fresh V1 Candidate follows
   ADR-0192's existing snapshot, retained-tail, prepare, transaction-current
   publication CAS, durable reread, immutable-view installation, and
   acknowledgement rules. Current-incarnation selected corruption continues through
   `RecordPublishedFailure` and `AllocateUnservableRebuildCandidate`; the reset
   is not a general corruption repair.
8. **Restore and authoritative formats do not change.** Destructive restore still advances
   only ADR-0072's authoritative incarnation. This transition performs no
   restore-time deletion or mutation, changes no durable encoding, registry,
   key, fixture, backup byte, public protocol, application behavior, or
   acknowledgement semantics, and adds no fallback to V1 or another generation.
   The quarantine name is derived cleanup state, never authoritative identity.

On acceptance, ADR-0192 Decisions 11 through 16 gain only the exact shape,
retention, startup ordering, repository operation, path retirement, witness,
and gated recovery rules in Decisions 1 through 8 above. Decision 17's inert
tag-65 compatibility bridge remains unchanged.

## Options considered

1. **Relabel the V1 artifact:** rejected because the frozen manifest has no
   incarnation and its rows may precede destructive restore.
2. **Delete tag-67 controls during restore:** rejected because it bypasses the
   control repository, changes restore authority, and creates a gap in the
   durable retention fence.
3. **Reuse an existing replacement operation:** rejected because every accepted
   replacement preserves the selected incarnation by design.
4. **Add one transaction-current reset plus exact collision retirement:**
   chosen because it is the smallest bounded authority that makes ADR-0160's
   mandatory rebuild reachable without reusing a pre-restore path.

## Consequences

- Restored stale derived state becomes safely rebuildable without being served.
- One additional internal control transition and crash matrix are required.
- Durable bytes, restore mechanics, and application-facing behavior remain
  unchanged.

## Standing design tests

- **Interface safety:** no application, operator, transport, or caller can
  select an incarnation or invoke the reset; only startup reconciliation can
  request the repository to use its transaction-current authoritative value.
- **Scale:** reconciliation reads one bounded control and one metadata value per
  at-most-256 source; rebuild remains streaming and bounded by existing limits.

## Checks

- `stale_history_incarnation_resets_columnar_control_before_serving` covers
  restore, cold open, no-serving/no-retention, and ordinary rebuild/publication.
- `columnar_history_reset_uses_transaction_current_incarnation` covers current,
  stale, future, mixed, malformed, exhausted, and caller-nonselection arms.
- `columnar_history_reset_unknown_commit_rereads_exact_control` covers Applied,
  mismatch, storage failure, unknown commit, repeated restart, and exact reread.
- `columnar_history_reset_retires_exact_colliding_candidate_paths` covers both
  exact paths, stale-plus-spec-drift, repeated construction crashes, every
  cleanup/rename/sync crash edge, kind/bound/reference failures, no parent scan,
  bounded deletion, no adoption/reuse, and exact final-candidate construction.
- `columnar_history_reset_precedes_spec_retarget` covers stale-plus-spec-drift,
  both durable rereads, closed gates, retarget, rebuild, publication, and ack.
