# WP-624 Contention-Edge Coalescing Gate

WP-624 tested ADR-0098's accepted two-millisecond completion-edge window on
the journal-authoritative standard profile. The candidate was rejected. It
formed substantially larger command frames and reduced summed writer work, but
made the public closed-loop workload slower by adding latency before each
contended dispatch.

The cloud diagnostic used the public gRPC TicketDesk interactive workload with
32 clients, three seconds of warmup, 15 measured seconds, and three fresh
process repetitions. It ran on the same isolated GCP persistent-disk profile
used by WP-620 and WP-621. This is a rejection cell, not PERF-018 evidence.

| Observation | Immediate completion edge | Two-millisecond candidate | Result |
| --- | ---: | ---: | --- |
| Mean throughput | 6,567 ops/s | 6,308 ops/s | **-3.9%** |
| Mean commands per frame | 7.31 | 12.91 | +76.6% |
| Representative writer-fence sum | 23,110,293 us | 8,552,819 us | lower, but not critical-path gain |
| Representative validation/encoding/staging | 6,526,159 us | 6,086,222 us | -6.7% |
| Aggregate p50 | baseline retained separately | 1.507 ms | candidate added collection latency |
| `create_comment` p50 | baseline retained separately | 25.166--26.214 ms | no public latency win |

All three candidate repetitions were correctness-clean. Their throughputs were
6,261, 6,314, and 6,348 operations per second. The frame counts were 1,296,
1,309, and 1,298 for 16,776, 16,919, and 16,706 logical commands.

The value-free candidate receipt is
`~/tmp/wp624-candidate-c32.json` on the inventoried VM, SHA-256
`d78105685a0bc1cf4089278ec277ea1090b13f9765a5c5bb889b6abc801f3a50`.
The immediate baseline remains `~/tmp/wp621-cloud/baseline-c32.json`, SHA-256
`55fe5228aebcb93bbe4db55b960f1c68865870003f50fa046233c56d83bbb7da`.

The result closes an attractive but wrong policy: reducing physical frame
count by waiting is not sufficient for interactive performance. The standard
profile therefore continues to dispatch accumulated completion-edge work
immediately and relies on the journal lane to co-fence frames that are already
ready. Idle, contended, hardened, barrier, shutdown, publication, and recovery
semantics remain exactly as before WP-624.

The next candidate must remove serial command-authority construction cost
without waiting. TicketDesk currently selects the anchored-event V4 command
segment, whose write path still builds the complete generated Prost object
graph. Streaming that already-fixed byte representation is the next measured
CPU lever; it changes no durable bytes or public behavior.
