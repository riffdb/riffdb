# WP-482 streaming command segments

The authoritative successful-command seal now streams canonical segment
headers, one capsule at a time, and one manifest entry at a time. It no longer
constructs a wire graph containing every capsule or clones every exact manifest
key before encoding. The independent generated-Protobuf encoder remains the
validation path for already sealed records.

This is an allocation and ownership change only. Command ordering, validation,
digests, durable bytes, acknowledgement, recovery, and public behavior are
unchanged. Regression tests prove exact byte equality for single-command and
multi-command segments, predecessor and non-default incarnation fields,
nonzero command ordinals, events, and a 59,000-entry preflight-accepted
manifest. Corruption continues to fail closed during decode.

Transaction-local serial command groups now emit one aggregate evaluation
observation and one aggregate validation/encoding/staging observation on their
successful path. Both carry the exact group command count and retain no
application values. This closes the evidence gap that previously left most
write-lane CPU inside undifferentiated writer-busy time.

## Public-path checkpoint

Fresh full-seed, per-session HTTP/2 diagnostics at 32 clients produced:

| workload | WP-481 | WP-482 | change | aggregate/write p50 |
|---|---:|---:|---:|---:|
| interactive | 21,297 ops/s | 21,611 ops/s | +1.5% | 0.344 ms / 7.864 ms |
| write-only | approximately 3,784 ops/s profiled | 3,946 ops/s mean (3 reps) | approximately +4.3% | 8.126 ms write p50 |

The three write-only repetitions were 3,944, 3,955, and 3,937 ops/s, all with
zero failures. The full 19,220-command seed completed in 2.53--2.67 seconds;
the interactive repetition completed its seed in 2.532 seconds.

During the 10-second interactive window, 3,183 successful serial groups
reported 1.116 seconds of aggregate evaluation time and 2.736 seconds of
aggregate validation/encoding/staging time. Writer busy time was 7.508
seconds. The next writer campaign should therefore target transaction-local
validation and staging ownership before revisiting durability mechanics.

Peak sampled RSS in the interactive repetition was 746 MB, down from 904 MB in
the WP-481 c32 checkpoint. Durable-file growth normalized by successful
mutation was 4,532 bytes versus 9,029 bytes in that earlier process run. Those
resource figures include allocator and recyclable-journal extent behavior and
are diagnostic rather than durable-format claims; byte-equality tests, not file
allocation deltas, prove format compatibility.

The public throughput gain is useful but not large. WP-482's main outcome is
removing whole-segment temporary ownership and making the remaining serial CPU
cost measurable. It does not claim PostgreSQL write parity.

Artifacts:

- `target/app-baseline/wp482-write32.json`
- `target/app-baseline/wp482-interactive32.json`

These are local diagnostic artifacts rather than published benchmark claims.

