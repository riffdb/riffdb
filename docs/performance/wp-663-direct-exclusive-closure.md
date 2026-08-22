# WP-663 direct exclusive lane closure

WP-663 is complete without production activation. Direct ownership materially
reduced steady-state generated point-read latency, but the verified-direct-TLS
probe did not reduce connection setup plus the first operation by the 25
percent required by ADR-0138. The diagnostic listener, framing crate, client
selector, benchmark selector, TLS ALPN, and all production-candidate code were
removed. Unary gRPC remains the generated-application default and `PERF-018`
is unchanged.

## Measurement identity

The loopback mechanics measurements exercised candidate revision `7410d39c`.
N1 built the exact release artifact once and the unchanged binaries ran on N1
and E2:

- `riffdbd`:
  `def4a632378b18336ed307c378e84a2f9b171b4f5e36655091c7a7216a756c27`
- `riffdb-client-transport-diagnostic`:
  `012b4cff881c382714847ebe5ee90748f0e8b85b4bde9d6a412622e7f6a2c739`

The verified-TLS measurement exercised revision `8240f68d`. N1 again built
the release artifact once and the unchanged binaries ran on both hosts:

- `riffdbd`:
  `c13c8bf14aadbee98be1f36901694a7f2e02506df83149d74dc8638e295abf14`
- `riffdb-client-transport-diagnostic`:
  `3db682cd13dbaa03729499cc4913ffb81f0eac6b1d76b840ecd4399f8ef6e68e`

Every cell used the full TicketDesk seed, one persistent c1 caller, 500 warmup
operations, 5,000 measured exact generated `GetTicket` operations, a fresh
database, and a fresh daemon process. The probe used the normal application
service and authorization path. Unary and direct carriage used the same trust
profile in each comparison. These cells are reject-first mechanics evidence,
not `PERF-018` release evidence.

## Loopback mechanics result

The loopback probe passed all three mechanics thresholds in all three
counterbalanced process generations on both cloud profiles:

| Host | Order | Setup + first-op reduction | Complete reduction | Outside-service reduction | Ledger error |
| --- | --- | ---: | ---: | ---: | ---: |
| N1 | after | 30.55% | 44.60% | 61.51% | 0.00033% |
| N1 | before | 34.13% | 45.64% | 63.06% | 0.00131% |
| N1 | after | 33.94% | 48.80% | 66.05% | 0.00070% |
| E2 | after | 33.41% | 54.58% | 66.29% | 0.00028% |
| E2 | before | 41.59% | 48.27% | 61.38% | 0.00026% |
| E2 | after | 31.29% | 44.56% | 59.03% | 0.00026% |

The required minima were 25 percent for setup plus first operation, 40
percent complete, and 55 percent outside service; the maximum ledger error was
five percent. Matching feature-disabled and enabled-but-unused controls showed
no greater-than-five-percent server-stage, RSS, CPU, wall-time, seed, startup,
shutdown, or unary regression. This result justified testing the same
mechanism under verified TLS; it did not authorize production implementation.

## Verified-TLS reject-first result

The first counterbalanced verified-TLS generation failed the setup threshold
on both hosts:

| Host | Unary setup + first op | Direct setup + first op | Reduction | Required | Complete reduction | Outside-service reduction | Ledger error |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 | 3,055.79 us | 2,683.97 us | **12.17%** | 25% | 48.51% | 64.82% | 0.00032% |
| E2 | 3,766.08 us | 2,928.84 us | **22.23%** | 25% | 52.37% | 63.70% | 0.00046% |

Direct ownership therefore removes most steady-state HTTP/2/client-scheduling
cost, including under verified TLS, but TLS connection establishment dominates
enough of the first operation that the complete candidate does not satisfy the
accepted lifecycle gate. ADR-0138 says any missed mechanics threshold stops
WP-663, so generations two and three and the production lane were not run or
built. Averaging the failed cell with later observations would violate the
reject-first rule.

## Receipts

The value-free JSON receipts remain outside the repository:

- loopback:
  `/home/user/tmp/wp663-cloud-results/{n1,e2}-r{1,2,3}.json`
- verified TLS: `/home/user/tmp/wp663-tls-{n1,e2}-r1.json`
- sorted eight-line receipt-manifest SHA-256:
  `c5d681e0490c136e2aac467039aeb4fa418288bbde11bafdc91681684ab9a10c`

The TLS receipt hashes are
`f3128dd0d33e746b9aa18c88c95deb643acbf4733cdf5e1db07de87de453cd53`
for N1 and
`1896c5913621d9b58c11137eab862423ef1ef845f49f3e5351f53227a122682c`
for E2.

## Decision

The candidate implementation commits were reverted in full. No direct
listener, framing crate, preface, ALPN, client API, generated selector,
driver-host selector, benchmark selector, public fixture, or customer
documentation remains. The accepted ADR records a falsified design; it does
not activate a compatibility surface.

WP-623 remains open. A future transport candidate requires a new accepted ADR
and a new reject-first gate rather than reviving the removed protocol.
