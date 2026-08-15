# WP-625 Streaming Authority Encoding Gate

WP-625 tested direct construction of the byte-frozen anchored-event V4 command
segment body. The candidate avoided building the complete nested generated
Prost graph and retained exact byte equality against the independent generated
encoder. The candidate was rejected because that work was not a material part
of the cloud critical path.

The cloud diagnostic used the public gRPC TicketDesk interactive workload with
32 clients, three seconds of warmup, 15 measured seconds, and three fresh
process repetitions. It ran on the same isolated GCP persistent-disk profile as
WP-621 and WP-624. This is a rejection cell, not PERF-018 evidence.

| Observation | Existing V4 encoder | Streaming candidate | Result |
| --- | ---: | ---: | --- |
| Mean throughput | 6,567 ops/s | 6,753 ops/s | +2.8% |
| Validation/encoding/staging | 6,526,159 us | 6,478,654 us mean | -0.7% |
| Writer-fence sum | 23,110,293 us | 23,397,302 us mean | +1.2% |
| Aggregate p50 | 1.704 ms | 1.704 ms | unchanged |
| Mean seed time | 11.280 s | 10.946 s | -3.0% |

All three candidate repetitions were correctness-clean. Their throughputs were
6,772, 6,762, and 6,726 operations per second. The candidate neither reached
the predeclared ten-percent public-throughput gate nor the twenty-percent
staging-time gate, so the streaming implementation and its live-path selection
were removed.

The value-free candidate receipt is
`~/tmp/wp625-candidate-c32.json` on the inventoried VM, SHA-256
`668f3dfe81e48f34dcceb806f2d12b286c71e82035603ae73bdb3148841b7e4a`.
The existing-writer baseline remains
`~/tmp/wp621-cloud/baseline-c32.json`, SHA-256
`55fe5228aebcb93bbe4db55b960f1c68865870003f50fa046233c56d83bbb7da`.

The result rules out another small serialization-only optimization. Cloud
staging time remains roughly six and a half seconds during each 15-second cell,
and writer busy time remains close to the whole measurement window. The next
candidate must move a substantial, immutable portion of command preparation
off the sole sequencing/apply coordinator. It may not pre-authorize, pre-assign
sequences, read mutable transaction facts early, or change durable bytes.
