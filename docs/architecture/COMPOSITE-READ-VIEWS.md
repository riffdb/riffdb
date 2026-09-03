# Composite read views

ADR-0104 defines the standard-profile state published to readers as one frozen
composite view:

1. one known-durable redb checkpoint; and
2. the complete, gap-free, durably fenced journal overlay after that
   checkpoint.

The overlay contains only complete canonical values and explicit tombstones.
A point read checks the overlay first. An overlay value replaces the checkpoint
value, a tombstone is authoritative absence, and a key unchanged by the suffix
falls back to the captured checkpoint. A bounded scan performs the equivalent
ordered merge and stops at the existing row and inspected-work limits.

The checkpoint root, overlay, database identity, history incarnation, record
registry digest, application and administration frontiers, and terminal frame
hash are one immutable object. A request captures that object once. It cannot
combine an old checkpoint with a new overlay or recapture an index generation
mid-request.

Before an overlay can freeze, RiffDB verifies:

- exact database, predecessor-frontier, and frame-hash continuity;
- bounded transition, encoded-suffix, and memory charges;
- canonical table keys and complete stored values;
- absence for inserts and the exact prior-value hash for replacements and
  tombstones; and
- ordered application of every mutation in each complete frame.

The compiled standard-profile ceilings are 8,192 transitions, 32 MiB of
cumulative encoded suffix bytes, and 128 MiB of conservative overlay-memory
charge. Checkpointing starts at the half-full point (4,096 transitions or
16 MiB) and earlier if the fixed 40-MiB extent must reserve one maximum padded
frame. One frame and the complete writer-private unpublished prefix remain
independently capped at 256 transitions and 16 MiB. These are fail-closed safety
bounds, not tuning settings.

Every staged frame is charged at its exact encoded and padded sizes before
submission. If an asynchronous checkpoint has not reclaimed enough logical or
physical headroom, the sole writer applies bounded backpressure: it waits for
that checkpoint, publishes the compacted successor, rebases the still-private
frame against the equivalent successor view, and then retries admission. Reads
continue from the last published composite view. RiffDB does not report an
ordinary capacity race as a successful write or as permanent audit failure.

Journal durability receipts may be observed by independent coordinator work in
a different order from submission. RiffDB therefore registers every sealed
frame in one bounded publication queue before releasing the sole-writer lease.
Any waiter advances the complete durable prefix through its own frame in
submission order; it cannot skip an earlier command or administration-audit
frame. Publishing a predecessor on another waiter's behalf advances only the
shared durable view. The predecessor's typed command or audit result remains
available to its original caller.

Physical checkpoint compaction copies a proven overlay prefix into redb. It
does not advance a logical index generation, so a cursor is invalidated only by
an actual application mutation, not by storage housekeeping.

Redb-writing barriers first materialize the complete published suffix. If a
derived worker reaches a barrier while a durable journal frame is still
awaiting publication, it observes transient writer backpressure and retries
without degrading authoritative application readiness. A successful barrier
advances the operational read root to its exact post-barrier state before
publishing a cache or result. Graceful shutdown drains the suffix, classifies
the optional validated-prefix checkpoint without mutating it or walking
population rows, then commits the bound clean-close lifecycle record as its
final authoritative mutation. The next startup durably consumes that clean
state before activating any writer.

## Current implementation boundary

The standard profile selects the composite path for command acknowledgement and
ordinary reads. WP-490 owns the remaining production performance gate and
operational evidence.

The hardened profile remains the independent two-phase redb oracle and never
selects the journal overlay.
