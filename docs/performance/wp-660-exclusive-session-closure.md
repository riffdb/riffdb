# WP-660 single-owner session mechanics closure

WP-660 is closed without production activation. The exclusive one-owner
candidate passed its live semantic smoke test and improved the N1 point-read
mean, but failed the predeclared cross-profile mechanics gate on E2. Its
production implementation was removed.

This package also found and fixed a diagnostic defect in the older bounded
session shadow: the diagnostic opened one bounded session and then created
fresh unary worker connections, so its reported session cell did not exercise
the selected session transport. The corrected driver now constructs and closes
one selected bounded session per worker.

## Mechanics gate

Both hosts ran the same detached `29434655` server artifact, full TicketDesk
seed, one generated `GetTicket`, 32 warmups, and 500 measured operations. The
candidate binary was built natively on each host. These short cells are
diagnostic evidence and are not PERF-018 qualification receipts.

| Host | Unary mean | Corrected bounded mean | Bounded gain | Exclusive mean | Exclusive gain | WP-660 result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| N1 | 592.426 us | 442.069 us | 25.38% | 436.517 us | 26.32% | candidate passes host threshold |
| E2 | 1,427.863 us | 952.767 us | 33.27% | 1,545.529 us | -8.24% | candidate fails host threshold |

The exit gate required at least a 20 percent exclusive-path improvement on
both cloud profiles and no greater-than-five-percent regression. The E2 result
fails both conditions. No exclusive-session client state, selector, benchmark
flag, or production dispatch remains.

The corrected existing ADR-0127 bounded session clears the narrow c1 mechanics
threshold on both hosts. That is an existence proof only, not activation: its
c8/c32, unary-command, cancellation, uncertainty, resource, seed, and frozen
comparator gates still require a separately registered package.

## Receipts

- N1: `/home/user/tmp/wp660-n1-c1.json`, SHA-256
  `5c5af29893f3be32fac0e8bb0aa9f14779a48763f6dfb96bc1796b0272540448`
- E2: `/home/user/tmp/wp660-e2-c1.json`, SHA-256
  `ec0c13c3cff6bc2c4ade08e6baddf16973fc80c9fef5796dc2e14de720a217da`

The receipt schema explicitly sets `evidentiary: false`,
`perf_018_eligible: false`, and `release_comparator_changed: false`.

## Safety result

The failed candidate changed neither command/query semantics nor the public
wire protocol. Removing it restores the previously accepted ADR-0127 client
implementation exactly. The retained diagnostic correction changes only which
already-public transport the non-evidentiary worker invokes; it adds no
authority, fallback, retry, freshness, ordering, or durability behavior.
