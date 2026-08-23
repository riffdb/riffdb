# WP-670 current-head cloud preflight

This report records the short, non-evidentiary current-head measurements used
to size WP-670. It does not qualify `PERF-018`, change the frozen comparator,
or convert a noisy favorable denominator into a release pass.

## Identity and method

The exact measured revision is `c8d2e5fb`. One release `riffdbd` and one
release app-baseline/transport-diagnostic artifact were built from that source
on each inventoried cloud profile. N1 (`bench-host-n1`) and E2
(`bench-host-e2`) ran in parallel; each host ran safe-application PostgreSQL
and RiffDB sequentially. All databases and build artifacts lived under
`/home/user/tmp` on persistent VM storage.

The unary matrix used 64 measured samples after 16 warmups. The mixed cell used
32 closed-loop clients for 15 measured seconds after a three-second warmup.
The exact `GetTicket` ledger used 500 measured calls after 64 warmups and also
ran the already accepted bounded-session shadow. These windows are deliberately
too short for release evidence.

## Public preflight

| Host | Mixed safe PG | Mixed RiffDB | Throughput ratio | PG p95 | RiffDB p95 | Tail ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 | 7,907 ops/s | 6,904 ops/s | 0.873x | 16.253 ms | 20.972 ms | 1.290x |
| E2 | 7,814 ops/s | 6,768 ops/s | 0.866x | 16.777 ms | 20.972 ms | 1.250x |

Every mixed operation completed successfully. There were zero conflicts,
idempotency mismatches, unavailable/overloaded outcomes, or other errors.

Representative unary p50 ratios remain outside the 1.10x gate:

| Scenario | N1 RiffDB / PG | E2 RiffDB / PG |
| --- | ---: | ---: |
| `point_get_ticket` | 2.07x | 2.60x |
| `point_get_user` | 2.50x | 3.79x |
| `list_tickets_by_project_status` | 2.73x | 3.47x |
| `list_open_tickets_for_assignee` | 2.80x | 3.65x |
| `ticket_detail_page` | 1.71x | 2.36x |
| `board_page_450` | 1.74x | 1.99x |
| `create_comment` | 1.40x | 1.40x |
| `open_ticket_with_labels` | 1.53x | 1.60x |
| full seed | 3.69x | 2.95x |

The PostgreSQL unary cells were slower and noisier than prior release
preflights. They therefore provide a conservative denominator for rejecting a
RiffDB pass, not evidence that PostgreSQL regressed or that any favorable cell
qualified.

## Exact point-read ledger

The current-head exact generated `GetTicket` ledger separates the synchronous
benchmark bridge, customer-paid async unary call, server stages, and
outside-server residual:

| Host/path | Caller mean | Server stages | Outside server | Bridge-only |
| --- | ---: | ---: | ---: | ---: |
| N1 unary | 638 us | 142 us | 492 us | 4 us |
| N1 async shadow | 572 us | 142 us | 430 us | n/a |
| N1 bounded session | 544 us | 124 us | 420 us | n/a |
| E2 unary | 1,857 us | 435 us | 1,409 us | 13 us |
| E2 async shadow | 1,050 us | 273 us | 777 us | n/a |
| E2 bounded session | 1,078 us | 264 us | 814 us | n/a |

The bridge is not the product gap. Outside-service transport and scheduling
remain the dominant small-read cost. E2 also exhibits large process-order
variance. WP-666 already falsified post-readiness derived catch-up as a
twenty-percent causal explanation, so this observation does not reopen a
settle/wait workaround.

ADR-0138/0139's removed direct-ownership candidate remains the only measured
existence proof of the required scale: established verified-TLS `GetTicket`
fell 48.51 percent on N1 and 52.37 percent on E2, with outside-service
reductions of 64.82 and 63.70 percent. Those accepted packages correctly
rejected their candidates under their proxy mechanics thresholds. WP-670 does
not reinterpret those failures or reuse their wire identity. ADR-0141 proposes
a new identity and makes actual public ratios, semantic equivalence, and
cross-generation stability the gate.

Transport cannot close `board_page_450` alone. Its current 1.74--1.99x ratio
survives even a perfect removal of the point-read residual. Revised ADR-0141
therefore makes WP-670 a deliberately narrow verdict: build the minimum safe
private candidate and run the complete unary matrix immediately. A service-
owned miss on any ordinary small operation ends transport work before
production hardening. A complete small-operation pass preserves the candidate
only as private diagnostic machinery and hands BoardPage50/200/450 attribution
to WP-671. Conditional WP-672 is the sole owner of later production hardening
and activation, and cannot start until the large-result gate has a measured,
accepted closure.

## Artifact custody

The value-bearing diagnostic JSON remains outside the repository under:

- `/home/user/tmp/wp670-c1-{pg,rd}-{n1,e2}.json`
- `/home/user/tmp/wp670-c32-{pg,rd}-{n1,e2}.json`
- `/home/user/tmp/wp670-transport-{n1,e2}.json`

No report contains credentials or application values in repository history.
