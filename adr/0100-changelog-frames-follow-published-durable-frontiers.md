# ADR-0100: Changelog Frames Follow Published Durable Frontiers (Amendment 1 to ADR-0093)

- **Status:** Accepted
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

Accepted by the maintainer on 2026-08-08, as revised against ADR-0101:
changelog frames follow published durable frontiers, dual-frontier
addressed; the replication implementation arc opens with the emitter
package against this boundary.

## Accepted Amendment 2 — Delete-aware entity transition frames

- **Status:** Accepted
- **Exact text accepted:** Yes, 2026-08-11
- **Date:** 2026-08-09
- **Amends:** this record's closed insert-or-replace entry algebra
- **Related:** ADR-0019 Amendment 1 (validated-prefix proof), ADR-0083
  (post-image-free entity references), ADR-0085 (retention tombstones),
  ADR-0101/ADR-0104 (local overlay tombstones), ADR-0107 (compiler-bounded
  checked deletion)

### Context

ADR-0107 permits command-time deletion only after replication and startup
validation can represent it exactly. The current changelog cannot: its V1
entries are puts, and an entity point-read after a published frontier returns
either the final post-image or absence. That is insufficient when one durable
publication covers multiple FIFO subgroups that touch the same entity. For
example, a create followed by a delete has no final entity row, while a delete
followed by a recreate has only the recreated row. A follower cannot infer or
validate either intermediate transition from the final snapshot.

This is not fixed by shipping local journal mutations. ADR-0100 and ADR-0101
deliberately separate journal bytes from the replication contract. The missing
authority must instead be retained in command history and exposed through a
versioned changelog algebra.

### Decision

#### 1. V1 remains immutable; deletion requires exact V2 negotiation

`ChangelogFrameV1` remains a put-only format. It is never reinterpreted to
accept a delete tag. A deletion-capable primary emits `ChangelogFrameV2`, and a
follower or bootstrap receiver must negotiate that exact format before any
frame or snapshot is transferred. A V1-only receiver refuses a V2 stream with
a typed `changelog_format_unsupported` result before apply; it never skips an
unknown transition. V2 uses a distinct magic/version domain and hash chain, so
V1 bytes cannot decode as V2 or the reverse.

The format rotation may activate only at a receipted frame boundary. The
receipt binds database ID, history incarnation, predecessor dual frontier,
prior V1 terminal hash, and initial V2 chain hash. Resume cursors name their
format, and no cursor crosses the rotation without that receipt.

#### 2. Command history carries every entity transition

The successor command capsule stores a bounded, canonically ordered
`CommittedEntityTransitionV1` for every entity mutation. Each transition binds:

- command sequence and mutation ordinal;
- canonical entity target and physical entity key;
- prior chain state: `NeverExisted`, `Live`, or `Deleted`, with prior chain
  revision, prior value hash when live, and prior transition hash when not the
  first transition;
- next chain state: `Live` with exact post-image version and hash, or `Deleted`;
- database ID and history incarnation through its containing command segment;
  and
- a domain-separated transition hash over every preceding field.

One command may transition a target at most once. Transitions are canonical by
target within a command and by `(command sequence, mutation ordinal)` across a
frame. Create is `NeverExisted -> Live`; update is `Live -> Live`; delete is
`Live -> Deleted`; recreate is `Deleted -> Live`. Chain revision increases by
exactly one for every transition and never resets after deletion.

Legacy post-image references remain readable for pre-rotation history. Every
post-rotation entity mutation must carry the successor transition form; a
mixed command segment or a missing transition is corruption. A rotation
checkpoint anchors the complete chain-head state at the V1/V2 boundary so
post-rotation validation never guesses a predecessor omitted by V1 history.

#### 3. One canonical entity-delete tombstone entry class

V2 adds exactly one delete entry class, `EntityDeleteTombstone`. Its payload is
the exact `Live -> Deleted` committed transition plus its transition hash. The
entry repeats the command sequence and mutation ordinal used for canonical
ordering, and the frame header supplies the database/history and published
frontier binding. Its physical key is the canonical entity key. It is distinct
in magic, tag, and hash domain from ADR-0085 retention-range tombstones and
ADR-0101/ADR-0104 in-process overlay tombstones.

V2 continues to carry final live entity rows as exact puts, but a follower
validates *all* committed entity transitions in sequence order before
materializing the frame's final current state. The commit entries supply every
intermediate live hash; the tombstone entry supplies every delete transition;
the final entity put, when present, must match the last live transition. This
proves create-update-delete-recreate histories even when intermediate
post-images are not materialized in the published snapshot.

A repeated complete frame is idempotent only when its frontier, chain hash,
and encoded bytes match the already-applied frame receipt exactly. At entry
level, an entity tombstone is accepted only when the current chain head equals
its complete prior state. Missing, duplicated, stale, reordered,
cross-history, prior-value-mismatched, or transition-hash-mismatched
tombstones fail the entire frame atomically.

#### 4. Entity chain heads are explicit authoritative current state

A registry-governed `entity_chain_heads` table holds one row per entity
identity ever transitioned after format rotation. Each row binds target,
chain revision, live/deleted state, current value hash when live, last command
sequence, and last transition hash. It contains no application field payload.

The commit coordinator updates the entity row, derived indexes, and chain head
in the same command transaction. A delete removes current entity/index rows and
writes the deleted chain head; a recreate writes the live entity/index rows and
replaces that head. The changelog follower performs the same transition while
applying its frame. Application reads never expose chain-head rows, and neither
an application nor MCP can submit them directly.

Bootstrap and backup enumerate `entity_chain_heads` as authoritative state.
The bootstrap manifest binds its row count and content digest. A receiver that
cannot represent the table refuses the bootstrap before installing any row.

#### 5. Validated-prefix proof distinguishes live rows from chain history

The V2 validated-prefix checkpoint replaces the current entity-only
fingerprint with a fingerprint over canonical sorted chain heads. It records
separately:

- live entity-row cardinality;
- deleted chain-head cardinality;
- total chain-head cardinality; and
- checked cumulative chain-transition count (the sum of chain revisions).

The fingerprint includes target, state tag, chain revision, current value hash
when live, last command sequence, and last transition hash. Checkpoint creation
walks current chain heads, not command history, so its cost is O(entity
identities) rather than O(transitions). Startup verifies live entity rows
against live heads, verifies absence for deleted heads, and validates every
post-checkpoint transition from the anchored head map.

Suffix reconstruction uses each first post-checkpoint transition's explicit
prior state rather than `entity_version - 1`. Create-update-delete-recreate is
therefore reversible to the checkpoint boundary without treating current table
length as historical transition count. Retention-range deletion below the
watermark remains separately typed and cannot create, satisfy, or remove an
entity chain head.

A V1 checkpoint is usable only before the receipted V2 rotation boundary. A
database containing a V2 command segment, delete tombstone, or chain-head row
with only a V1 checkpoint falls back to full mixed-history validation from the
rotation receipt; it never accepts V2 state under the V1 `len() ==
count-at-bound` proof.

#### 6. Follower and bootstrap apply are atomic and fail closed

For each V2 frame the follower first validates identity, format, frontier,
chain, checksums, canonical order, commit/tombstone reciprocity, and every
entity transition against an in-transaction chain-head view. Only after the
complete frame validates does it apply authoritative puts, entity/index
removals, and chain-head replacements in one storage transaction. A crash
leaves the predecessor frame or the complete successor frame.

Bootstrap transfers one verified snapshot at a named V2 frame boundary,
including chain heads and the rotation receipt, followed by frames whose
predecessor is exactly that boundary. Resume and replay use the same validator;
there is no bootstrap-only tolerance for a missing or mismatched tombstone.

### Consequences

- Entity deletion becomes an ordinary immutable command transition while
  current state remains materialized; RiffDB does not become an event-sourced
  database.
- One additional authoritative row is maintained per post-rotation entity
  identity. This is the bounded cost of preserving delete/recreate chain
  identity without scanning history on every checkpoint or mutation.
- Changelog V2 and checkpoint V2 are compatibility boundaries. Their fixtures,
  registry migration, rotation receipt, backup enumeration, crash matrix, and
  mixed-version refusal ship in WP-559 before the compiler can emit a delete.
- Replication transport remains independent: `ShipChangelog` carries negotiated
  frames and does not inherit journal encoding or storage-engine bytes.

### Required acceptance evidence

1. Create, update, delete, recreate, and a same-publication
   create-then-delete/delete-then-recreate corpus reconstruct byte-identical
   follower current state and chain heads.
2. Missing, duplicate, stale, reordered, cross-database, cross-incarnation,
   prior-value-substituted, and transition-hash-substituted tombstones refuse
   the complete frame without partial apply.
3. V1/V2 stream and bootstrap mismatches refuse before mutation; the receipted
   boundary resumes exactly after restart.
4. A V2 checkpoint opens create-update-delete-recreate history through its fast
   path; corrupting live count, deleted count, transition count, state tag,
   value hash, or chain hash refuses or takes the documented full-validation
   fallback, never a successful stale open.
5. Kill arms cover rotation receipt persistence, delete frame apply, chain-head
   replacement, checkpoint write, and follower acknowledgement.
6. Compiler and runtime keep delete unavailable until all preceding evidence is
   green.

### Acceptance

Proposed for maintainer review on 2026-08-09. No durable encoding, registry
entry, follower apply rule, or delete command may land under this amendment
until its exact text is accepted.
