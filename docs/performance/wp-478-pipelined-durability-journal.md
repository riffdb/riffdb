# WP-478 Pipelined Durability Journal

WP-478 targets the remaining synchronous write-path ceiling without weakening
RiffDB's acknowledgement guarantee. The current standard profile blocks its
sole ordered writer on every redb durable fence. At 32 mixed-load clients,
RiffDB sustains about 16,077 operations per second against PostgreSQL safe-app's
32,380; RiffDB reads have the lower median, while command latency is about 11
milliseconds versus 4 milliseconds.

Same-host mechanics isolated the relevant boundary:

| Candidate | 1,000 representative groups |
|---|---:|
| redb one-phase `Immediate` | 5.0--5.4 s |
| sequential journal + redb `None` | 5.1--5.4 s |
| fewer redb tables | 4.9--5.1 s |
| pipelined grouped journal + redb `None` | 0.14--0.41 s |

The pipelined probe used 27--76 durable journal flushes instead of 1,000. It is
the first measured candidate with enough headroom to close the application
comparison rather than merely move it by a few percent.

ADR-0101 therefore defines standard-profile authority as a known-durable redb
checkpoint plus an exact gap-free journal suffix. Applied redb roots remain
private until their frames are durable. A successful journal fence publishes
the final covered snapshot before any outcome, notification, transient index,
projection, outbox work, subscription update, or response is released.

The first interface increment includes:

- a closed table and mutation vocabulary;
- before-image hashes for recovery-time compare-and-apply;
- database identity, sequence range, predecessor hash, bounds, and checksums;
- length-delimited torn-tail detection;
- a bounded no-delay journal lane that drains only ready frames and groups one
  `sync_data`; and
- mutation capture beside the exact redb writes for command state, allocator,
  and linked audit rows.

Startup now owns the journal before structural validation. It accepts exactly
two redb positions: the file header's checkpoint, which causes the complete
suffix to be compare-and-applied and checkpointed, or the suffix tail left by a
crash after checkpoint and before reclamation, which must match every final
table value. Any other frontier, before-image mismatch, hash-chain defect, or
database mismatch refuses open. Reclamation uses a synced sibling replacement,
an atomic rename, and a parent-directory sync; a torn terminal frame alone is
discarded.

This page does **not** claim that the production command path is journal-backed
yet. Production enablement remains gated on coordinator pipelining, publication,
backup integration, the process crash matrix, and the retained c1/c32/c128
comparison. Until those gates pass, application writes retain the documented
redb durability path.
