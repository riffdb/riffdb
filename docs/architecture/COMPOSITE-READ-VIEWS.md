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

Physical checkpoint compaction copies a proven overlay prefix into redb. It
does not advance a logical index generation, so a cursor is invalidated only by
an actual application mutation, not by storage housekeeping.

## Current implementation boundary

WP-487 provides the closed engine-neutral overlay values plus redb and memory
conformance adapters. Production command acknowledgement and ordinary reads do
not select the composite path yet. WP-488 owns publication after the journal
fence, WP-489 owns asynchronous checkpoint/recovery/barriers, and WP-490 owns
production enablement and the performance gate. Until those packages pass,
the existing redb-snapshot path remains active.

The hardened profile remains the independent two-phase redb oracle and never
selects the journal overlay.
