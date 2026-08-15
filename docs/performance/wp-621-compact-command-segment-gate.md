# WP-621 Compact Command-Segment Activation Gate

WP-621 tested the exact raw-or-LZ4 successor accepted by ADR-0123 and
ADR-0125 against the real TicketDesk app-baseline corpus on the inventoried
four-core GCP profile. The byte reduction passed. The required cloud writer
time improvement did not. The compact record is therefore **not activated**,
the durable identity remains alpha epoch 1 writer 1, and current command
segments remain byte-identical.

This is a falsified optimization, not a waived gate. The retained change is a
fixed-cardinality writer-frame census that can measure a later candidate
without retaining keys, values, principals, symbols, or per-command labels.

## Corpus and method

The diagnostic cell used the public gRPC TicketDesk workload with 32 closed-
loop clients, three seconds of warmup, and 15 measured seconds. It ran on the
same GCP VM and persistent ext4 block device inventoried by WP-620. The baseline
used revision `2571e370`. The candidate used the accepted LZ4 block, complete
7/8 envelope-selection rule, checked uncompressed length and SHA-256, and the
same command, journal, checkpoint, and acknowledgement semantics.

The short window is sufficient to reject the candidate but is not PERF-018
release evidence. A candidate that cannot pass this diagnostic may not consume
the three-repetition 90-second qualification budget.

| Observation | Current writer | Compact candidate | Result |
| --- | ---: | ---: | --- |
| Throughput | 6,567 ops/s | 6,517 ops/s | -0.8% |
| Validation/encoding/staging | 6,526,159 us | 6,679,133 us | +2.3% |
| Durable writer | 23,110,293 us | 24,410,252 us | +5.6% |
| Combined required stage time | 29,636,452 us | 31,089,385 us | **+4.9%** |
| Compatible writer groups | 2,409 | 2,266 | candidate had fewer groups |
| Logical commands committed | 17,599 | 17,230 | correctness-clean |

The activation threshold required at least 20% lower combined stage time. The
candidate instead regressed by 4.9%, despite receiving the favorable variance
of fewer physical groups. This rejects the mechanism as a cloud write-latency
optimization.

The candidate's live frame census still proves that compression was effective
as compression:

| Corpus | Complete frame bytes | Raw-equivalent frame bytes | Reduction | Selected segment bytes | Raw segment bytes | Reduction |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| c32 measured process | 34,272,406 | 53,982,911 | 36.5% | 9,565,953 | 29,276,458 | 67.3% |
| 19,220-command seed | 30,558,825 | 52,951,543 | 42.3% | 8,292,454 | 30,685,172 | 73.0% |

Both byte gates pass. They do not compensate for the failed stage-time gate.
On this storage profile, the reduced payload did not reduce the durable fence
enough to pay for compression and format complexity. The unary and smallest-
frame CPU gates were not used to rescue the candidate after the independent
c32 hard gate failed.

## Evidence identity

The value-free JSON receipts remain on the isolated VM:

- baseline: `~/tmp/wp621-cloud/baseline-c32.json`, SHA-256
  `55fe5228aebcb93bbe4db55b960f1c68865870003f50fa046233c56d83bbb7da`;
- candidate: `~/tmp/wp621-cloud/candidate2-c32.json`, SHA-256
  `a21ddca9db8c5d6cbab761b508df8a4d9c78a84d3543c2d551a08c9f157febc5`.

The candidate process reported zero public errors, conflicts, unavailable
results, overloads, or idempotency mismatches. The raw stdout census is the
source for the size table because the candidate benchmark binary predated the
final JSON census field.

## Retained instrumentation

At clean shutdown the daemon emits one closed six-value census:

1. successfully fenced command frames;
2. logical commands in those frames;
3. selected complete frame bytes;
4. raw-equivalent complete frame bytes;
5. selected command-segment bytes; and
6. raw command-segment bytes.

All accumulation uses checked fixed-cardinality counters. A frame containing
multiple command segments sums every segment in order. Counts advance only
after the journal fence succeeds. The current writer reports selected and raw
as equal, so the instrumentation changes neither bytes nor codec work.

## Decision

ADR-0123's activation rule is enforced exactly:

- no V5 record is registered;
- no LZ4 dependency enters the production graph;
- no durable manifest, registry, fixture, or release edge changes;
- no writer upgrade or downgrade ceremony is introduced; and
- WP-623 must benchmark the unchanged writer until a separately accepted
  mechanism demonstrates the required cloud gain.

The result narrows the next campaign: command-segment bytes dominate retained
space, but cloud c32 latency is governed by per-group fence/application cost,
not by those bytes at the tested sizes. A follow-up should target fewer durable
groups or less authoritative apply work before revisiting compression.
