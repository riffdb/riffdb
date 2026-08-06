# WP-463: Allocation-free read-dependency reciprocity

Complete command-record construction proves that the read dependencies already
owned by the durable commit are exactly the dependencies evaluated by the
command runtime. Before WP-463, that reciprocal check converted the live set
into a second durable collection, cloning targets, structurally decoding index
prefixes, sorting the result, and then comparing it with the commit-owned set.

WP-463 compares the two already canonical collections directly. The comparison
checks collection length, dependency kind, entity target, index identifier,
index-prefix bytes, and expected entity or epoch state. Differing kinds,
targets, states, or lengths still fail closed. Insertion order remains
irrelevant because both public constructors canonicalize their input before the
comparison can run.

This is an in-memory validation optimization only. Transaction-current read
validation still runs at admission and commit, and the authoritative commit
still stores the same dependency records. Durable bytes, transaction ordering,
recovery, authorization, and aggregate bounds do not change.

## Evidence

The exact warm parent revision was `816989ce`. With generated concurrency 128
and three internal samples, its full public TicketDesk seed median was 3.553 s.
The candidate median was 3.450 s, a 2.9% improvement. Captured candidate writer
traces spent 0.886-0.902 s in validation/encoding/staging, compared with about
0.951 s for the warm parent trace. Representative unary `create_comment` p50
improved from 2.719 ms to 1.727 ms.

The shared host has variable filesystem latency, so these measurements are
retain-or-revert evidence rather than a new release baseline. The structural
result is deterministic: complete record validation no longer constructs the
comparison collection, while semantic tests cover both dependency kinds,
target and state mismatches, canonical insertion-order independence, and
length mismatches.
