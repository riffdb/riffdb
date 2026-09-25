# WP-760 hand-off (2026-09-25)

Working note for whoever closes WP-760. Supersedes the "remaining reds" and
"gaps" sections of `wp760-adaptation-report.md`.

## State

- `cargo test -p riffdb-storage-redb --all-features`: every target passes,
  including `storage_recovery_matrix` (176/0, ~110 s). The matrix was not run
  during the earlier adaptation rounds; at `0390a557f` it had 26 failures and
  one hang, all WP-760 regressions (it passed at `f590478fb`).
- Production-scale dirty restart reaches readiness on the bounded path
  (`process_to_ready` 24 ms and 44 ms, zero transient index rebuilds, ~37 MB
  RSS at ready); before `f5e6eefb5` it refused with `LimitExceeded`.

## Decisions taken (list under `decisions_taken` at closure)

1. **Bounded dirty header** (`f5e6eefb5`, implementer, D-003). The bounded
   dirty path proves the administration allocator against the durable tail
   only (last AUDIT row and head command capsule audits); gap-free continuity,
   the physical/derived pair comparison, and last-activation reciprocity move
   to the background scrub. First access already refuses a disagreeing pair.
2. **Fast-path proofs re-homed to followers** (`a65d405b3`, maintainer, in
   session, 2026-09-25). A primary's complete-walk checkpoint fast path no
   longer runs; a follower's does, on the checkpoint bootstrap carries over.
3. **Under-reported checkpoint counts accepted on a primary** (`a65d405b3`,
   maintainer, in session, 2026-09-25). A possible-but-wrong prefix count
   admits the bounded dirty path, which never consumes it; the next complete
   walk corrects it, and a follower's complete walk refuses it. Pinned by
   `under_reported_checkpoint_counts_fail_closed_on_the_complete_walk_only`.

## Known hazard: cold transient indexes and the mutation gate

Recorded without a code change (maintainer, in session, 2026-09-25).

Clean-certificate startups (since ADR-0157) and ADR-0182 bounded dirty
startups leave the transient command indexes cold. Their lazy rebuild
(`SharedRedb::ensure_transient_indexes_ready`) acquires the mutation gate and
checkpoints the published journal suffix. Consequently:

- A read that triggers the rebuild on the thread that holds an open deferred
  epoch deadlocks: the gate is a FIFO ticket lock and the holder is waiting on
  itself. Reproduced by `deferred_command_root_is_invisible_until_the_tail_fence_publishes_it`
  before its warm-up.
- The same read after an epoch is sealed but before it publishes returns
  typed `Unavailable` backpressure from the journal barrier.

No production path was found that reads on the thread holding an open epoch.
A same-thread fail-fast is not sound: leases move between threads by design
(for example `run_step` in `replication_bootstrap_receiver.rs` hands its owner
to a new `spawn_blocking` task each step), so the gate cannot distinguish
re-entry from a lease another thread now carries. Sound options, if a
production path appears: eager post-readiness warm-up on a background lane
(narrows the window only), or a typed gate-held capability that the lazy
rebuild refuses at compile time (needs a decision record).

Related fix already landed: `a3c561ff3` stopped the outbox reciprocal read
from misclassifying command-owned events as corrupt when the index is cold.

## Remaining for closure

- Public offline scrub per ADR-0203 (OBL-0182-4).
- Release evidence and `scripts/check-dirty-recovery-evidence` (OBL-0182-6):
  dirty-restart wall time and peak heap at the 65,536-row and production-scale
  checkpoints, and scrub throughput.
- Handbook pages for the bounded dirty path and the scrub health states.
- Closure block with `completed_at` and the decisions above.
