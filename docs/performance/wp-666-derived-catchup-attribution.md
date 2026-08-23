# WP-666 derived catch-up interference attribution

WP-666 is complete without a production change. Current-HEAD profiling showed
that the `riffdb-columnar` worker overlaps immediate post-restart point reads
while replaying durable command segments. A bounded diagnostic-only settle
control then falsified that overlap as the material unary-latency mechanism on
both inventoried cloud profiles. The control was removed; neither the frozen
comparator nor production readiness, scheduling, freshness, validation, or
worker behavior changed.

## Candidate identity

The diagnostic was staged on `cdd10303` and deliberately not committed. It
added only a reported `--post-restart-settle-seconds 0..120` control to
`riffdb-client-transport-diagnostic`; zero retained the existing behavior and
nonzero slept after each fresh daemon generation. Its source SHA-256 was
`a2c11bf6a976eeb3238399181abf2c7bf832eb2cce710861eb95497a0a50139c`.

N1 built one release artifact which ran unchanged on N1 and E2:

- `riffdbd`: `406de8ac6aa2449cf36a8e1293e3ac2a1c1e181bb56005c3965adc3842b3ad19`
- diagnostic: `003ea314aa0b1e6e3e439d8d00f834b22c9195192e4832ba8d785f2b6b34f529`

Each cell used the full TicketDesk dataset, one persistent caller, 32 warmups,
500 exact generated `GetTicket` measurements, and fresh daemon generations.
The settled arm waited 40 seconds after readiness. That delay is a diagnostic
control and cannot be credited as product gain or PERF-018 evidence.

## CPU and lifecycle finding

An N1 system-wide software-CPU profile placed the immediate read window beside
the `riffdb-columnar` thread. Its dominant user work was durable command-frame
SHA-256, wire preflight, CRC, Protobuf decode/encode, allocation, and storage
reads. The worker's `apply_available` call scans all currently available
64-record pages before its outer loop observes shutdown. Consequently the
immediate control took roughly 90 seconds end to end even though each shape
issued only 532 reads: daemon shutdown waited for the in-progress derived
catch-up pass.

This is a real bounded-lifecycle follow-up, but CPU coexistence is not itself a
latency cause. The paired settle experiment below supplies that causal test.
The raw `perf.data` is retained only on the N1 host because it is approximately
1.25 GiB and contains host/process symbols; it is diagnostic, not release
evidence.

## Paired causal result

| Host / shape | Immediate mean | Settled mean | Change | Immediate service | Settled service |
| --- | ---: | ---: | ---: | ---: | ---: |
| N1 synchronous unary | 618.427 us | 615.421 us | **0.49% faster** | 157.799 us | 156.280 us |
| N1 asynchronous unary | 581.894 us | 570.724 us | **1.92% faster** | 151.019 us | 148.305 us |
| E2 synchronous unary | 1,031.766 us | 986.606 us | **4.38% faster** | 237.192 us | 224.075 us |
| E2 asynchronous unary | 984.364 us | 1,079.292 us | **9.64% slower** | 258.979 us | 264.568 us |

The predeclared causal threshold was a 20-percent caller reduction on both
profiles. Neither synchronous cell approaches it; the asynchronous E2 arm
moves in the opposite direction. Service-stage changes are likewise small.
Waiting for derived catch-up therefore cannot close the unary gate and must not
be added to the product or benchmark.

## Receipt custody

Value-bearing JSON remains outside the repository:

- N1 immediate: `/home/kevin/tmp/wp666-n1-immediate.json`, SHA-256
  `f592ef8d7a22f400898cdcefb341dcf3a46a821e054e40a10a582ef7af2789f1`
- N1 settled: `/home/kevin/tmp/wp666-n1-settled40.json`, SHA-256
  `63e3df329e201ffd210d89a8d87fcf1193052f8895ec6f8a8f6704debc9de76e`
- E2 immediate: `/home/kevin/tmp/wp666-e2-immediate.json`, SHA-256
  `6a0f9e0c2a962926952e86fc436053cc5bd072ed5d091845be98990a6edce311`
- E2 settled: `/home/kevin/tmp/wp666-e2-settled40.json`, SHA-256
  `f5b66a253afab9a8a2e8cf7355ca4e180c0e55765ab097560f030b0e516398d4`

## Decision

Columnar catch-up remains semantically independent derived work and ordinary
named reads continue not to wait for its frontier. Its current whole-prefix
pass should later become interruptible at a bounded page boundary so shutdown
does not wait tens of seconds behind a large replay. That is a lifecycle and
resource-fairness package with its own projection equivalence tests, not an
argument for delayed readiness or a unary performance claim.

WP-623 remains open. The remaining unary gap is still split between roughly
150--260 microseconds of API-neutral service work and the larger customer-paid
Tonic/HTTP2 scheduling/protocol residual. A next candidate must target those
measured components directly rather than background catch-up, entity storage,
the benchmark bridge, or another response router.
