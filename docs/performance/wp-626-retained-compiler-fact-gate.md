# WP-626 Retained Compiler-Fact Gate

WP-626 tested retaining the exact compiler-derived input-fact proof from
command preparation through transaction-current validation and index
derivation. The candidate removed four repeated full fact reconstructions and
their normalized-input clones from each successful mutation. It was rejected
because the public and stage improvements were too small to justify widening
the private command-state chain.

The cloud diagnostic used the public gRPC TicketDesk interactive workload with
32 clients, three seconds of warmup, 15 measured seconds, and three fresh
process repetitions. It ran on the same isolated GCP persistent-disk profile as
WP-621 through WP-625. This is a rejection cell, not PERF-018 evidence.

| Observation | Existing derivation | Retained-proof candidate | Result |
| --- | ---: | ---: | --- |
| Mean throughput | 6,567 ops/s | 6,825 ops/s | +3.9% |
| Validation/encoding/staging | 6,526,159 us | 6,158,284 us mean | -5.6% |
| Writer-fence sum | 23,110,293 us | 23,398,428 us mean | +1.2% |
| Aggregate p50 | 1.704 ms | 1.769 ms | +3.8% |
| Mean seed time | 11.280 s | 10.758 s | -4.6% |

All three repetitions were correctness-clean. Their throughputs were 6,869,
6,855, and 6,751 operations per second. The candidate reached neither the
predeclared ten-percent public-throughput gate nor the twenty-percent staging
gate, so the retained private fields, alternate validation/index entrypoints,
and architecture-pin amendments were removed.

The value-free candidate receipt is
`~/tmp/wp626-candidate-c32.json` on the inventoried VM, SHA-256
`804698941c4862847299695a5d81e6294f6a3acae5c169860b2926ec364e4827`.
The existing-writer baseline remains
`~/tmp/wp621-cloud/baseline-c32.json`, SHA-256
`55fe5228aebcb93bbe4db55b960f1c68865870003f50fa046233c56d83bbb7da`.

Together WP-625 and WP-626 show that isolated encoding and fact-reconstruction
work account for only single-digit improvements. The next diagnostic is an
inherited CPU profile of a short, fixed cloud cell. Any next activation must
remove or parallelize a dominant serialized stack rather than accumulate these
rejected micro-optimizations.
