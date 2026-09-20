---
adr: "0246"
title: Derived Sink Exemptions And Restore
status: accepted
tier: guarantee
date: 2026-09-19
accepted: 2026-09-19
acceptance: 'maintainer, in session, 2026-09-19: "Approved"
  (ADR-0246 as written)'
requires: [ADR-0240, ADR-0102]
amends:
  - ADR-0240 by naming the one structural exemption to its decision 2 gate
    prohibition, and by requiring restore to account for the derived sidecar
    its decision 3 introduces.
supersedes: []
requirements: []
packages: [WP-791]
obligations:
  - id: OBL-0246-1
    package: WP-791
    proof: only_an_unpublished_store_may_apply_derived_state_under_the_gate
    says: A derived-state apply may hold the primary mutation gate only on a
      store handle that cannot serve a command commit, checked structurally
      rather than by the mode the caller believes it is in.
  - id: OBL-0246-2
    package: WP-791
    proof: restore_leaves_no_derived_state_from_the_replaced_timeline
    says: A restore leaves behind no derived state belonging to the database it
      replaced.
review_triggers:
  - A derived-state path would take the primary mutation gate on a store that
    can serve command commits, justified by the mode it is believed to be in.
  - Restore or retirement would replace a fixed file set that does not account
    for every derived store the database owns.
  - A derived store would be treated as anything other than rebuildable.
---
# ADR-0246: Derived Sink Exemptions And Restore

## Context

Implementing ADR-0240 surfaced two questions its text does not answer, both
found by review rather than by tests.

**The gate prohibition met a path that still takes the gate.**
`store_follower_projection.rs:117` acquires the primary mutation gate and opens
a primary write transaction to stage a projection replay. The implementation
justified this on the grounds that follower and bootstrap replay have no
command commit waiting on that gate. ADR-0240 decision 2 forecloses exactly
that form of argument — it calls the requirement structural, "not a performance
target" — and decision 6 applies it to every derived-state mechanism. The
justification is also not durably true: `RedbFollowerApplier` is driven by
online archive restore (`maintenance/archive_preparation.rs`), where the
database is live.

**Restore does not know the sidecar exists.** ADR-0240 decision 3 moved derived
state out of the primary store. Restore replaces a fixed three-file set —
database, journal, durable-format marker (`archive_reconciliation.rs:127-137`)
— and nothing in the tree removes the derived sidecar. A point-in-time restore
therefore leaves the pre-restore sidecar in place. The existing disagreement
check only fires when derived state claims *more* than the primary has, so once
the restored database is written past the stale sidecar's frontier, rows
derived from the replaced timeline's commits are served as current: a decision 5
violation with no detection.

## Decision

1. **One exemption, and it is structural.** A derived-state apply may hold the
   primary mutation gate only on a store handle that **cannot serve a command
   commit** — a bootstrap candidate, an unpromoted follower, or a staging
   directory that is not yet published. The exemption is a property of the
   handle, enforced in the type or by a checked precondition, never an
   assertion about the mode the caller believes it is in.
2. **Online restore does not qualify.** A path that operates on a live,
   published database is not exempt, whatever it is replaying. If online
   archive restore needs to stage derived state, it does so through the same
   sidecar path as any other apply.
3. **Restore accounts for derived state.** A restore, retirement or any other
   operation that replaces a database's files must leave behind no derived
   state belonging to the database it replaced. Because derived state is
   rebuildable by decision 1 of ADR-0240, **removing the sidecar is always a
   correct answer** and is the default; carrying a matching sidecar across is
   permitted only if it is proven to belong to the restored timeline.
4. The file set a restore replaces is derived from what the database owns, not
   from a fixed list. A new derived store added later must not silently fall
   outside it.

## Options considered

**Allow the mode-based exemption as implemented** was rejected. It is the
argument decision 2 exists to refuse, and the review found a live path
(`archive_preparation.rs`) where the belief is already false. An exemption that
depends on the caller being right about its own context will be wrong the first
time a caller is reused.

**Forbid the gate absolutely, with no exemption** was considered and not taken.
Bootstrap and follower replay build a store nothing can yet commit to; requiring
a sidecar path there adds a second store to a database that has no readers, for
no safety gain. The exemption is real, and worth naming precisely rather than
leaving implementers to infer it.

**Carry the sidecar across restore, matching it to the restored timeline** was
rejected as the default. It requires proving the derived state belongs to the
timeline being restored, which is strictly more work than rebuilding it, for a
cache. It remains permitted where that proof exists.

## Consequences

Restore becomes slightly more expensive: derived state is rebuilt rather than
inherited, so the first queries after a restore see `Building` until the
rebuild completes. That is the cost of not serving another timeline's rows, and
ADR-0240 already made the rebuild path load-bearing.

Naming the exemption structurally means the boundary proof cannot be satisfied
by a file list. A proof that walks the call graph must resolve callees across
files and fail loudly where it cannot, because the exempt and non-exempt paths
now differ by the handle they hold rather than by where they live.
