# WP-632 Frozen-Client Transport Attribution

WP-632 separates the frozen synchronous benchmark bridge from the real
generated asynchronous application call. It does not amend `PERF-018`, replace
the release comparator, or produce release evidence. Both shapes execute the
same generated `GetTicket` operation over a persistent Tonic HTTP/2 channel;
the asynchronous shape is a diagnostic shadow only.

Each synchronous sample records one paired tuple:

- total caller-observed duration;
- Tokio runtime-entry duration;
- generated asynchronous operation duration; and
- runtime-exit duration.

The sum of runtime entry and exit is classified as measurement artifact. The
generated asynchronous operation, including client encoding/decoding, Tonic,
HTTP/2, service ingress, authorization, and query execution, is customer-paid
product cost.

## Frozen transport fact

The exact pinned Tonic dependency is `0.14.6`. Its client endpoint and server
builder both default `TCP_NODELAY` to true. An executable
`riffdb-client-rust` test freezes the client endpoint fact; source inspection of
the exact dependency freezes the server default. RiffDB does not override
either default in this path.

## Cloud results

Both isolated GCP hosts ran the release diagnostic at full TicketDesk scale
with 500 measured and 32 warm-up operations per client. The hosts were otherwise
idle. Throughput is diagnostic operations per second, not a `PERF-018` score.

| Host | Clients | Sync ops/s | Async ops/s | Async gain | Sync p50 | Async p50 | Bridge p50 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 Intel | 1 | 268 | 276 | 3.0% | 3.670 ms | 3.408 ms | 8.7 us |
| N1 Intel | 8 | 1,319 | 1,409 | 6.8% | 6.029 ms | 5.505 ms | 8.2 us |
| N1 Intel | 32 | 1,512 | 1,521 | 0.6% | 20.972 ms | 20.972 ms | 8.7 us |
| E2 AMD | 1 | 331 | 356 | 7.6% | 3.015 ms | 2.884 ms | 6.7 us |
| E2 AMD | 8 | 1,870 | 1,939 | 3.7% | 4.063 ms | 3.932 ms | 5.9 us |
| E2 AMD | 32 | 2,101 | 2,389 | 13.7% | 15.204 ms | 13.107 ms | 5.9 us |

The predeclared activation thresholds were at least 15% at c=1, at least 10%
at c=8, and no regression at c=32. Native async fails the materiality threshold
at c=1 and c=8 on both CPU families. The bridge is 0.2--0.3% of the
low-concurrency call, so removing it cannot be booked as closing the product
gap. The E2 c=32 improvement is useful scheduler evidence, but it neither
repairs the failed low-concurrency cells nor changes the comparator freeze.

## Customer-paid decomposition

Server-stage means are additive instrumentation within one operation. The
client/transport residual below is the paired generated-call mean minus the sum
of the server-stage means. It includes HTTP/2 and Tonic work, queueing before
and after the instrumented handler, client Protobuf work, and generated result
decoding; it is not all removable transport overhead.

| Host | Clients | Generated call mean | Server-stage sum | `execute` mean | Client/transport residual | Server share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 Intel | 1 | 3.717 ms | 2.977 ms | 2.830 ms | 0.740 ms | 80.1% |
| N1 Intel | 8 | 5.995 ms | 4.476 ms | 4.346 ms | 1.519 ms | 74.7% |
| N1 Intel | 32 | 20.948 ms | 5.031 ms | 4.925 ms | 15.917 ms | 24.0% |
| E2 AMD | 1 | 3.010 ms | 2.208 ms | 2.075 ms | 0.802 ms | 73.3% |
| E2 AMD | 8 | 4.253 ms | 2.854 ms | 2.762 ms | 1.399 ms | 67.1% |
| E2 AMD | 32 | 15.071 ms | 3.495 ms | 3.391 ms | 11.576 ms | 23.2% |

At c=1 and c=8, exact authoritative query execution is the largest measured
cost. At c=32, most caller time is outside the instrumented handler stages,
consistent with transport/runtime queueing and orchestration. A bounded
multiplexed session therefore has a real target at medium concurrency, but
cannot alone close the low-concurrency cloud gap: even deleting the entire
c=1 residual leaves the 2.2--3.0 ms server path.

These exact generated-operation timings must not be substituted for the mixed
interactive workload's stage averages. They intentionally isolate one public
generated `GetTicket` shape and reveal that its authoritative `execute` cost
needs its own attribution package.

## Decision

1. Reject "make the frozen driver async" as a performance candidate. It is a
   small measurement correction and remains non-evidentiary.
2. Keep proposed ADR-0127 as a justified additive product experiment. Its
   predeclared candidate gate remains c=1 +15%, c=8 +10%, no c=32/seed/unary or
   tail regression beyond 5%, and semantic parity. Exact ADR acceptance is
   required before implementation or any `PERF-018` amendment.
3. Queue a separate exact-`GetTicket` server-execution attribution package.
   It must split snapshot acquisition, route/index lookup, entity decode,
   dependent result assembly, policy revalidation, and response release before
   proposing a semantic or storage change.
4. Neither package alone is assumed to satisfy `PERF-008`. After each candidate
   passes its own materiality gate, rerun fresh same-campaign PostgreSQL and
   RiffDB comparisons under the explicitly amended comparator, if any.

## Evidence receipts

- N1 report: `~/tmp/wp632-cloud/n1-report.json`, SHA-256
  `f9b7c078706f70e982956373f7f57327de16ccf203695f794e34e58fad417797`.
- E2 report: `~/tmp/wp632-cloud/e2-report.json`, SHA-256
  `49c9cdf52577dddea44f3fbc5705e2402d65874eaf157a882c5cee06f993202a`.
- Both reports declare schema `riffdb.client-transport-attribution/v1`,
  `evidentiary: false`, `perf_018_eligible: false`, and
  `release_comparator_changed: false`.

## Documentation impact

None. The diagnostic binary, paired timings, proposed ADR, and this report are
maintainer-only performance evidence. They add no application-visible
operation, option, protocol field, metric label, or compatibility promise.
