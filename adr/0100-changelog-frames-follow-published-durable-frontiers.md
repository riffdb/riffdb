# ADR-0100: Changelog Frames Follow Published Durable Frontiers (Amendment 1 to ADR-0093)

- **Status:** Proposed
- **Date:** 2026-08-07
- **Decision owners:** RiffDB maintainers
- **Amends:** ADR-0093 (replicated availability via authoritative changelog
  shipping)
- **Related:** ADR-0101 (pipelined standard-profile writer journal — the
  durability boundary this record aligns to; its §6 reserves exactly this
  decision), ADR-0098 (bounded durability epochs), ADR-0061
  (acknowledgement after durability), ADR-0082 (single total order)

## Context

ADR-0093 defined the unit of replication as the authoritative changelog
entry for one commit sequence, and required the stream to be exact,
gap-free, and resumable. It was written when every committed sequence was
individually durable at commit time: committed, durable, and shippable were
one predicate evaluated at one instant.

ADR-0098 and then ADR-0101 ended that identity for the standard profile.
Authoritative state is now a redb checkpoint plus an exact durable journal
suffix; subgroups apply through `Durability::None` and become durable only
when a journal flush covers them; publication of every dependent effect —
responses, notifications, projections, outbox work, subscribers — waits for
that flush (ADR-0101 §4). Between apply and flush, a commit sequence exists
in the writer's private roots but is **not part of the durable prefix**: a
crash erases it, by design and with process-kill evidence.

Unamended, ADR-0093 is now dangerous rather than merely stale. An emitter
that ships per-sequence entries as they apply would ship unflushed state; a
primary crash before the covering flush would leave a follower holding
sequences the primary's own recovery says never happened. The follower
would be *ahead of* the authoritative durable prefix — precisely the
divergence class ADR-0093's exactness requirement exists to make
unrepresentable. This amendment is a correctness requirement of composing
ADR-0093 with ADR-0101, not a tuning choice.

ADR-0101 §6 anticipated this record: the local journal is not a replication
protocol, and a replication frame may be derived only from published
durable journal frontiers. This record is the reserved decision.

The standing scale test (co-located storage, single-node memory, full-state
rewrite) is applied per section; no answer changes from ADR-0093.

## Decision

### 1. The changelog frame is one published durable-frontier advancement

The unit of shipping, checksumming, acknowledgement, and follower apply is
the **changelog frame**: the complete set of ADR-0093 changelog entries for
every commit sequence and administration sequence newly covered when the
primary publishes a durable frontier advancement — one successful journal
flush on the standard profile, or one `Immediate` commit on the direct
singleton path and the hardened profile (a single-group frame).

Within a frame, ADR-0093 §1's per-sequence entry definition and exactness
are unchanged — the frame adds a boundary, never blurs attribution. Like
the journal frames it follows, a changelog frame advances the **dual
frontier**: it names its predecessor and covered application
commit-sequence frontier and its predecessor and covered
administration-sequence frontier, encoding an unchanged frontier
explicitly. A frame header carries those four values, the frame's entry
counts, and the frame checksum. Gap-free now means: contiguous frames whose
predecessor frontiers equal the prior frame's covered frontiers, with no
sequence outside a frame.

### 2. The emitter derives frames from published durable state only

The changelog emitter observes durable-frontier publications and derives
frame contents exclusively from published durable snapshots — the same
reader discipline ADR-0101 §4 imposes on every dependent effect. The
emitter never reads writer-private applied roots or unflushed subgroup
state under any circumstance, including read-ahead or speculative framing.
When implemented, the emitter joins the enumerated gated consumers
(responses, notifications, projections, outbox, subscribers) as a peer
obligation, pinned by the same class of visibility-gate tests.

Per ADR-0101 §6, the local journal's frames are not the wire format: the
changelog frame is derived from published durable state, not copied from
journal bytes. That the two boundaries coincide is the design; that the two
encodings could someday be unified is a possible future optimization
requiring its own amendment, and nothing here assumes it.

### 3. Followers apply one changelog frame per storage transaction

A follower applies each frame atomically: one storage transaction per
frame, all entries or none, in frame order. This mirrors the primary's
crash contract ("the preceding frontier or the complete next durable
prefix, never a partial command graph") onto the follower by construction —
a follower crash mid-apply recovers to a frame boundary, and the
acknowledged frontier of ADR-0093 §4 (the retention fencing input) is
therefore always a frame boundary. Followers never split or re-batch
frames: re-batching would give the follower a crash atomicity the primary
never had, and divergence under paired crashes.

Frame size is bounded by the flush-cover ceiling (ADR-0101 §3: at most 256
logical writer transitions and 16 MiB of encoded bytes per flush), so the
follower's apply transaction is bounded by a ceiling the primary's storage
engine already accepts.

### 4. Resume, tokens, and freshness: unchanged contract, sharpened grain

- Resume is from any acknowledged **frame** boundary; a frame is named by
  its covered dual frontier (ADR-0093 §6's sequence-addressed resume
  becomes frontier-pair-addressed).
- Commit tokens are minted at acknowledgement, which ADR-0101 holds until
  the covering flush succeeds; every token a client can hold refers to a
  published durable sequence. Follower `Causal` reads wait on the applied
  frontier exactly as ADR-0093 §3 states, with no new cases.
- Replication visibility is quantized to durable-frontier publications. The
  added latency is bounded by the flush cadence — negligible against
  asynchronous replication lag, and stated here so nobody rediscovers it as
  a surprise.

### 5. Promotion and vocabulary

ADR-0093 §5's promotion ceremony operates on frames without modification:
"complete applying its received prefix" means "complete applying received
complete frames"; a partial frame in transit at fencing time is discarded,
never applied.

Three near-collisions are named once so code and prose never interchange
them: **journal frames** (ADR-0101 — local durable encoding) are not
**changelog frames** (this record — the derived replication unit);
**durability epochs** (ADR-0098 — apply-to-flush spans) are not
**leadership epochs** (ADR-0093 §6 — promotion counters). A changelog frame
corresponds to one or more journal frames covered by one published flush;
a leadership epoch outlives millions of durability epochs.

## Consequences

- The ADR-0093 feasibility prototype's acceptance criteria are updated:
  prefix exactness (criterion 1) is compared at frame boundaries over the
  dual frontier; the kill-resume probe (criterion 2) must additionally kill
  mid-frame on both sides and show no partial frame is ever applied or
  acknowledged; the promotion drill's RPO (criterion 3) is measured in
  sequences but realized in whole frames.
- The emitter inherits a ready-made attachment point: the durable-frontier
  publication edge of ADR-0101 §4. No polling, no second bookkeeping
  structure — the publication event is exactly the emitter's input.
- Followers inherit the standard profile's recovery-suffix bounds
  transitively; no new ceiling is invented for replication.
- A future synchronous tier (ADR-0093 §8) would acknowledge at published
  durable frontiers too; nothing here forecloses it.

## Rejected alternatives

- **Per-sequence shipping of applied-but-unflushed entries** — ships state
  the primary's recovery can erase; follower divergence on primary crash;
  the motivating hazard of this record.
- **Shipping local journal frames as the wire format** — explicitly
  reserved against by ADR-0101 §6; couples the replication protocol to the
  local durable encoding and its recovery-oriented contents; revisitable
  only as its own amendment.
- **Emitter-side buffering of unflushed entries** (read early, release at
  flush) — duplicates flush-cover tracking outside the storage engine and
  creates a second source of truth for what a flush covered; the
  publication edge already knows.
- **Follower re-batching of frames** — breaks the mirrored crash contract;
  discussed in §3.

## Acceptance criteria

Absorbed into the ADR-0093 prototype criteria as amended in Consequences;
no separate prototype. The emitter gating obligation (§2) additionally
requires, at implementation time, a falsifiable test in the style of the
journal visibility gates: an applied-but-unflushed subgroup constructed and
held open must be invisible to the emitter's framing pass, and neutering
the gate must turn exactly that test red.

## Acceptance

Pending maintainer review.
