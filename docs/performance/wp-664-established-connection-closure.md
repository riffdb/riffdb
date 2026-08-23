# WP-664 established-connection lane closure

WP-664 is complete without production activation. The corrected ADR-0139
lifecycle accounting passed its setup and first-operation gates in the first
two loopback generations on both cloud profiles, but the candidate was not
stable across three E2 generations. The third E2 loopback generation missed
both steady-operation gates, and the first E2 verified-TLS generation also
missed the accepted-session first-operation gate. The matrix stopped at the
first conclusive reject-first result. Every diagnostic listener, frame,
ALPN, client, selector, and benchmark implementation was then removed. Unary
gRPC remains the only generated-application transport and `PERF-018` is
unchanged.

## Measurement identity

The candidate was commit `7b93da0e`. N1 built the exact release artifacts
once; the unchanged bytes ran on N1 and E2:

- `riffdbd`:
  `cd77ed2d5c7f0e425bc18654dd503c9789ca35e935a33d55be53177c20915219`
- `riffdb-client-transport-diagnostic`:
  `6741be3aa4c68ac74e8bd9a652271c3b9b858cc10a94e19367b65f210d73bd41`

Every completed cell used the full TicketDesk dataset, 500 warmups, 5,000
measured exact generated `GetTicket` operations, and a fresh daemon/database.
Trust establishment, exact authenticated application-session establishment,
and the first generated operation were disjoint. A separate caller timer
closed their sum, while resumed reconnect was reported in a different field
and could not substitute for a cold full-handshake cell.

The thresholds were: cold trust plus session at most 105 percent of unary,
first accepted-session operation reduction at least 25 percent, steady
complete reduction at least 40 percent, steady outside-service reduction at
least 55 percent, and ledger error at most five percent.

## Reject-first results

| Host | Carriage/order | Setup ratio | First-op reduction | Complete reduction | Outside-service reduction | Ledger error | Result |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| N1 | loopback/after | 71.61% | 26.21% | 44.31% | 61.66% | 0.00031% | pass |
| N1 | loopback/before | 74.91% | 25.63% | 48.28% | 64.06% | 0.00098% | pass |
| E2 | loopback/after | 58.66% | 36.25% | 50.42% | 63.60% | 0.00102% | pass |
| E2 | loopback/before | 60.28% | 30.49% | 40.50% | 57.01% | 0.00063% | pass |
| E2 | loopback/after | 64.31% | 26.96% | **34.66%** | **53.25%** | 0.00012% | reject |
| E2 | verified TLS/after | 78.98% | **24.40%** | 55.85% | 66.21% | 0.12285% | reject |

N1's third loopback cell and the remaining TLS cells were interrupted once E2
made rejection conclusive. No missing cell is interpreted as a pass.

## Receipt custody

The completed value-bearing JSON receipts remain outside the repository:

- N1 archive:
  `/home/user/tmp/wp664-n1-7b93da0e.tar`, SHA-256
  `4104a20719c39040578c522d65edffa713517bc13e55928acbdab1c61387a508`
- E2 archive:
  `/home/user/tmp/wp664-e2-7b93da0e.tar`, SHA-256
  `ae777c5be2124050dbdc5cca4af5df3cfb97133e371c6b08e60635cd04e4d5a3`

Receipt SHA-256 identities, ordered by cell:

- N1 loopback after:
  `225e1f7e2d65057426ae53aeba94ae5ffe9543a6893421bf6a9638fbf6fd7d34`
- N1 loopback before:
  `422d035c94b371fad9a2884958c1a620b8663ef8cd58a21e8fc2ac439e885c59`
- E2 loopback after 1:
  `7fbbce9a05bdc3b95ac1f262c3354cdf60c682cd8050f92333526f1bd4cd708a`
- E2 loopback before:
  `e75a1a8c3d6e68e81f22c076ef8df8d3571a24abfc3ba8b12a4d5254e9e9da2c`
- E2 loopback after 2:
  `211204d5cd6a29d6c9f6d50f1cfd6150c20c5ad915c7fb612ef2fc1e0f531a7e`
- E2 verified TLS after 1:
  `4a008a74ad57a415971cf786b11ed77104237cfdb574697f268e1bc9c0a03df8`

## Decision

The revised accounting was useful: it proved that cold trust was not the
candidate's limiting mechanism and that direct ownership frequently removed a
large share of established-operation overhead. It did not prove the accepted
cross-generation stability or first-operation minima. RiffDB therefore does
not acquire a second generated-operation protocol for alpha. Any future
transport proposal starts from a new accepted ADR and new protocol identity;
these rejected bytes are not a compatibility surface.
