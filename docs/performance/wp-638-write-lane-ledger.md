# WP-638 closed public write-lane ledger

WP-638 re-baselines the post-WP-637 public application path and closes one
successful `CreateComment` from the generated client through the gRPC adapter,
API-neutral service, coordinator, deferred journal fence, publication, response
encoding, and acknowledgement. It changes no command, ordering, conflict,
durability, authorization, uncertainty, transport, or comparator semantics.

The measurements in this report are short diagnostic cells. They do not
replace PERF-018's 90-second, three-repetition release evidence.

## Frozen public re-baseline

All comparator cells use the frozen synchronous generated-client transport and
safe-application PostgreSQL obligations. PostgreSQL reported
`synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, and
`wal_sync_method=fdatasync` on both cloud hosts. Every cell below completed with
zero errors, conflicts, unavailable results, overloads, or idempotency
mismatches.

| host | clients | RiffDB ops/s | safe-app PG ops/s | RiffDB / PG |
|---|---:|---:|---:|---:|
| workstation | 1 | 2,097 | 1,795 | 1.17x |
| workstation | 8 | 13,621 | 10,628 | 1.28x |
| workstation | 32 | 29,360 | 38,573 | 0.76x |
| N1 | 1 | 969 | 1,648 | 0.59x |
| N1 | 8 | 4,744 | 7,957 | 0.60x |
| N1 | 32 | 6,675 | 8,672 | 0.77x |
| E2 | 1 | 605 | 1,013 | 0.60x |
| E2 | 8 | 4,380 | 7,577 | 0.58x |
| E2 | 32 | 6,623 | 9,569 | 0.69x |

The workstation cells are the completed three-repetition tri-backend run
`tri-20260816T135850Z`. The cloud cells are three-repetition, sequential,
exclusive backend phases from revision `df740e7e`. N1 used the harness-managed
PostgreSQL container. E2 used the host PostgreSQL service in a separate
PG-only phase; PostgreSQL was then stopped before the RiffDB-only phase.

The representative full-scale unary medians show that the cloud gap is not one
uniform mechanism:

| operation | N1 PG | N1 RiffDB | ratio | E2 PG | E2 RiffDB | ratio |
|---|---:|---:|---:|---:|---:|---:|
| point ticket | 0.385 ms | 0.699 ms | 1.82x | 0.845 ms | 1.325 ms | 1.57x |
| point user | 0.299 ms | 0.592 ms | 1.98x | 0.496 ms | 1.275 ms | 2.57x |
| tickets by project/status | 0.362 ms | 0.769 ms | 2.12x | 0.548 ms | 1.406 ms | 2.56x |
| open tickets by assignee | 0.456 ms | 1.100 ms | 2.42x | 0.651 ms | 1.749 ms | 2.69x |
| comments for ticket | 0.329 ms | 0.660 ms | 2.00x | 0.621 ms | 1.206 ms | 1.94x |
| project members | 0.313 ms | 0.646 ms | 2.06x | 0.569 ms | 1.122 ms | 1.97x |
| ticket detail | 0.864 ms | 0.817 ms | 0.95x | 1.681 ms | 1.236 ms | 0.74x |
| board 50 | 1.796 ms | 1.858 ms | 1.03x | 1.055 ms | 2.295 ms | 2.18x |
| board 200 | 4.427 ms | 4.271 ms | 0.96x | 1.608 ms | 4.393 ms | 2.73x |
| board 450 | 2.747 ms | 8.054 ms | 2.93x | 2.177 ms | 8.014 ms | 3.68x |
| create comment | 2.400 ms | 3.795 ms | 1.58x | 4.274 ms | 5.373 ms | 1.26x |
| close with comment | 2.503 ms | 3.602 ms | 1.44x | 4.636 ms | 5.660 ms | 1.22x |
| swap member roles | 2.135 ms | 3.133 ms | 1.47x | 4.548 ms | 4.955 ms | 1.09x |
| open with labels | 2.229 ms | 3.820 ms | 1.71x | 4.729 ms | 6.318 ms | 1.34x |

The full seed was 10.948 seconds versus 1.586 seconds on N1 and 9.364
seconds versus 2.519 seconds on E2. This remains a separate high-concurrency
writer problem; the unary ledger below does not reinterpret it as fence time.

## Fixed c=1 write ledger

Revision `44b628d8` adds fixed-cardinality, payload-free process-generation
histograms. The service stages are non-overlapping. The commit-call duration is
split into journal submission and the remaining asynchronous durability fence;
it is never counted twice. The client/HTTP2 remainder is client-observed mean
minus the complete server stage sum. It includes the socket, tonic scheduling,
client decode, and benchmark-call return that cannot be timestamped in the
server process. Including that explicit remainder closes both ledgers exactly;
the server-only sums are within 4.99% on the workstation and the larger N1
remainder agrees with WP-632's independent cloud transport attribution.

| component, mean per command | workstation | N1 | classification |
|---|---:|---:|---|
| client/HTTP2 remainder | 154.7 us | 530.3 us | outside writer; product transport cost |
| gRPC request adaptation | 5.9 us | 19.0 us | outside writer; movable adapter work |
| API-neutral service preparation | 50.3 us | 170.9 us | outside writer; bounded parallel work |
| coordinator queue | 211.9 us | 259.3 us | load-dependent queueing |
| admission transition | 9.7 us | 33.8 us | admission-ordered |
| compatibility/conflict ownership | 1.1 us | 3.8 us | sequencing-essential |
| deterministic evaluation | 84.9 us | 238.5 us | movable under PERF-008 |
| current validation, encoding, staging | 99.1 us | 281.8 us | admission-ordered as one current stage; must be split by proof before movement |
| final apply and journal submission | 100.9 us | 347.7 us | sequencing-essential as one current stage |
| asynchronous durability fence | 2,361.6 us | 625.9 us | durability-essential; not writer occupancy |
| publication | 0.1 us | 1.2 us | ordering-essential, post-fence |
| coordinator receipt/handoff remainder | 16.4 us | 58.7 us | sequencing-adjacent; no movement credited |
| API-neutral finish | 5.0 us | 15.1 us | outside writer |
| response encoding | 1.3 us | 4.7 us | outside writer; movable adapter work |
| **client-observed mean** | **3,102.8 us** | **2,590.7 us** | closed total |

The writer was busy for 305.5 us per accepted command on the workstation and
934.3 us on N1. The directly named serialized N1 stages (admission,
compatibility, evaluation, validation/staging, journal submission, and
publication) sum to 906.7 us, leaving 27.6 us of writer-loop overhead. The
larger 58.7 us coordinator-await remainder also includes receipt/fence handoff
and cross-thread scheduling. The 625.9 us N1 fence and 2,361.6 us workstation
fence occur after deferred submission and therefore do not consume that
serialized writer lane.

This classification is deliberately conservative. The broad current
`validation_encoding_staging` stage contains both deterministic work that a
future proof may prepare elsewhere and sequence/current-state operations that
cannot move. No part of that 281.8 us is credited as movable in this report.
Only the separately measured deterministic evaluation stage is currently
authorized as movable without first introducing a finer semantic proof.

## N1 c=32 queue and service split

The matched 15-second interactive diagnostic completed 101,064 measured
operations at 6,733 ops/s with zero failures. `CreateComment` mean/p50/p95 were
21.862/22.020/31.457 ms. Across the complete measured process generation:

| component | mean |
|---|---:|
| client/HTTP2 and response remainder | 1.829 ms/write |
| gRPC request adaptation | 0.020 ms/write |
| service preparation before coordinator wait | 0.504 ms/write |
| coordinator storage queue | 4.245 ms/write |
| post-dequeue coordinator service through publication | 15.222 ms/write |
| service finish and response encoding | 0.042 ms/write |
| client-observed `CreateComment` | 21.862 ms/write |

Writer duty was 16.559 of 18.025 process seconds, or 91.9%. Writer busy time
was 0.908 ms per accepted command, matching the 0.934 ms N1 c=1 quotient.
Completion groups averaged 7.06 commands. A physical commit group averaged
9.027 ms, of which 2.402 ms preceded journal receipt creation and 6.625 ms was
the deferred fence. These group durations are shared by their members and are
not divided into the public per-write latency ledger.

The c=32 result therefore has the classic saturated-single-lane shape. Queue
delay grows from 0.259 ms at c=1 to 4.245 ms at c=32, while serialized writer
service stays approximately 0.9 ms per command. Durability fencing contributes
public latency but runs outside writer occupancy. A batching-window or weaker
durability policy would target the wrong ceiling.

## Decision boundary for the next package

WP-638 authorizes no coordinator implementation. It establishes these bounds
for maintainer review:

- Moving only the already-separated deterministic evaluation removes at most
  238.5 us from N1's 934.3 us serialized command cost: a 25.5% reduction and a
  queue-capacity upper bound of about 1.34x before another ceiling appears.
- Applied proportionally to the measured cloud c=32 cells, that bound is about
  8,900--9,000 ops/s. It is sufficient in principle to cross the 0.90x
  safe-application gate on both N1 (7,805 ops/s required) and E2 (8,612 ops/s
  required), but it leaves little E2 headroom and is not a performance claim.
- A stronger candidate must split deterministic validation/encoding from the
  final current-state check, sequence assignment, conflict ownership, and final
  apply. It may move only a compiler-declared bounded preparation whose proof is
  revalidated in admission order. It must not turn global ordering into a new
  semantic requirement where ADR-0059 requires only conflict-domain ordering.
- The next package must predeclare public throughput and write p50/p95 gates per
  host from its measured serialized-time reduction. Read, seed, correctness,
  durability, uncertainty, and tail distributions remain no-regression gates.

ADR-0127 transport implementation, derived-consumer pay-once work, Python or
TypeScript comparators, compression, batching windows, entity caches, and lazy
decode remain explicitly out of scope.

## Receipts

All receipts are outside the repository under `~/tmp`; cloud originals remain
under `~/tmp` on their respective hosts.

| receipt | SHA-256 |
|---|---|
| workstation tri merged | `5c230ce5ebf0ec4c40cfb952ef640956982870419d73740f59e204e9635bd6c8` |
| N1 public rebaseline | `76480fbb777db80fcdacbb3cad1bc4f2e52bb26daf0d590010c87efce90a84d8` |
| E2 safe-app PG rebaseline | `e7d035507320db03d7b893b08514434d33f998f32895f3ea0e5da915abf97dd0` |
| E2 RiffDB rebaseline | `a4fe58c20229ab4650b59c04d2486c91f45237c8765afc7b858c775f3cdb7bdb` |
| N1 unary | `12142dc53825b43a96cb5e97a64642c3bebed10d053d0ddc2916a0dac988eb71` |
| E2 unary PG | `8967e2a8349793bda8e1b9d13028fab1badff698c20fe381df3d8a8c70ae13f2` |
| E2 unary RiffDB | `a2755f7a2c9f6b9506f87d812ddb03e8e31e8d6eba08760154989aa62fc25faa` |
| workstation c1 write ledger | `7efb7f1e090c97217ba0dbb72fd3a616478126fec22ad6c60082064e507f41ab` |
| N1 c1 write ledger | `5f59c03b27667b5389732ab63df414eaff435c8b4337888613ad58d34d670b53` |
| N1 c32 write split | `db7e2f2765638b3b52cb0d61bcab5db09a43ba7437fe43b1cef2fe099bc6783c` |

## Automated coverage

The instrumentation is exercised through the production service telemetry,
commit telemetry, process graph, real `riffdbd`, public gRPC client, and strict
app-baseline shutdown parser. Fixed cardinality and exact stage names are
covered by parser and observability tests. The targeted workspace tests,
app-baseline workspace tests with the matching daemon, formatting, and changed
crate checks pass at revision `44b628d8`.
