# WP-476 Constant-Time Durable Shape Dispatch

WP-476 reduces CPU inside the durable Protobuf structural-preflight boundary. It
does not skip or weaken that boundary.

Every closed durable message shape now constructs a fixed field-number dispatch
table at compile time. Internal declarations fail compilation if they exceed 16
rules, use field zero or a field above the closed field-20 ceiling, or duplicate
a field. During preflight, each parsed field selects its rule with one bounded
table lookup instead of scanning the shape's rule slice.

The existing allocation-free wire cursor still parses every field. The same
recursion-depth, visited-field, packed-item, occurrence, wire-type, byte-width,
UTF-8, and nested-message checks run with the same error classification. Unknown
fields remain ignored only by this structural pass; the unchanged deterministic
decode/re-encode boundary still rejects alternate durable encodings and unknown
fields before a durable record is accepted.

A recursive test compares the direct lookup against the authoritative rule
slice for every field number in every root and nested durable shape. Existing
golden envelopes, malformed vectors, canonicality tests, resource bounds, schema
hashes, and recovery tests remain the compatibility oracle.

## Same-host evidence

The full public 19,220-command seed was run three times immediately before and
after the change. Mean seed time fell from 2.165 seconds to 2.092 seconds, about
3.4%. Representative `create_comment` p50 fell from 1.909 ms to 1.678 ms. The
candidate retains the change because it improved the full path without changing
durable bytes or validation semantics.

Artifacts:

- `target/app-baseline/wp476-control.json`
- `target/app-baseline/wp476-direct-durable-dispatch.json`
