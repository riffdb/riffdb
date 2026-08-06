# WP-461: Single-owner staged event collection

Every successful event-emitting command constructs one complete checked durable
event collection. Before WP-461, the transient atomic record graph retained one
event-descriptor vector directly and another inside the authoritative commit
record, even though construction required them to be exactly equal. Event
payload records and their canonical encodings were already shared internally.

WP-461 makes the commit record the single owner of the staged event collection.
The atomic graph continues to expose the same event slice. Its constructor now
derives reciprocal outbox-intent descriptors from that checked collection, so a
caller cannot supply a second event or outbox vector. Event derivation, outbox
identity, provenance, commit-membership, ordering, and aggregate-bound checks
still run before storage. Current durable records and recovery behavior do not
change.

## Evidence

The exact parent revision was `ce8af01c`. Full public TicketDesk seeds used
generated concurrency 128 and three internal samples per report. Two parent
runs measured 6.139 s and 6.257 s. Three retained-candidate runs measured
6.105 s, 6.423 s, and 6.381 s. The ranges overlap on the noisy shared host; the
latest adjacent comparison was +2.0%, below the 5% material-regression bound.
Representative unary measurements fluctuated in both directions, from
5.04-6.36 ms, without a repeatable regression.

Two retained-candidate writer traces measured validation/encoding/staging at
1.283 s and 1.312 s, versus 1.307 s for the adjacent parent trace. An attempted
`Arc<[Event]>` collection was rejected before completion because it raised the
targeted stage to 1.401 s; the retained implementation keeps the existing
`Vec<Event>` in the commit and removes only the redundant sibling collection.

Semantic storage, commit, memory-backend, and all 58 process-level recovery
tests pass. Durable Protobuf encodings and transaction behavior are unchanged.
