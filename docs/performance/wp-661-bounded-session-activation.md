# WP-661 corrected bounded-session activation decision

WP-661 is complete without release activation. The corrected diagnostic proves
that ADR-0127's bounded session can materially improve some cells, but the gain
is not stable across both cloud CPU profiles. Unary remains the generated-client
default and `PERF-018` is unchanged.

## Measurement correction

The prior diagnostic opened a bounded session and then called a helper that
created fresh unary worker connections. The retained correction makes transport
selection part of worker construction and closes the selected bounded session
inside that worker. A `--bounded-session-first` diagnostic flag counterbalances
process/cache order. Both flags remain non-evidentiary and cannot alter the
release comparator.

## Valid mechanics observations

All cells use detached server revision `29434655`, the full TicketDesk seed,
one generated `GetTicket` operation, native builds, separate daemon process
generations, and no simultaneous benchmark on the host. Values below are
client-observed mean latency; lower is better.

| Host/order | c1 unary | c1 session | Change | c8 unary | c8 session | Change |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| N1, unary first | 573 us | 416 us | 27.4% faster | 826 us | 593 us | 28.2% faster |
| N1, session first | 554 us | 462 us | 16.6% faster | 808 us | 626 us | 22.5% faster |
| E2, unary first | 796 us | 789 us | 0.8% faster | 840 us | 921 us | 9.6% slower |
| E2, session first | 1,004 us | 1,135 us | 13.1% slower | 1,172 us | 1,053 us | 10.2% faster |

An earlier independent valid c1 observation recorded N1 at 592/442 us and E2
at 1,428/953 us. It established that the mechanism can win, but does not cure
the order/repetition instability above.

The ADR-0127 activation gate requires at least 15 percent c1 and 10 percent c8
throughput improvement on both hosts. E2 misses c1 in both counterbalanced
observations and misses c8 when unary runs first. Because the narrow mechanics
gate fails, WP-661 stops before expensive PostgreSQL and 90-second release
sweeps. A favorable c32 observation cannot override a low-concurrency gate.

One attempted counterbalanced pair was accidentally overlapped with a second
diagnostic process on both hosts. Those files are excluded completely; they are
not listed as receipts and no number from them appears above.

## Receipts

- N1 unary-first 1/8/32:
  `1aee25f3ed5b8f67d30baeb209709dfb618433b7d3a9ece7a52f35ee63eeb2dd`
- E2 unary-first 1/8/32:
  `c4fb0de2befd307fe25bbb7446f0bd8cf301cd7126723fbe8e867a11c98bbbce`
- N1 session-first 1/8:
  `f95c09c16b03aa61be4213fdde7a23a80964e16af4e644e46ffaaab40ec91699`
- E2 session-first 1/8:
  `b12e12992e3d3c08a54ce1d3b43f04157eec11494f2555954a2f51a47679e1d2`

Raw receipts remain under `/home/user/tmp/` and declare
`perf_018_eligible: false`. They contain no credentials or application values.

## Decision

The accepted session protocol and its explicit opt-in API remain available.
No application default, fallback, connection count, in-flight limit, retry
behavior, or comparator shape changes. No authentication or authorization
decision is cached. The retained diagnostic correction is measurement-only and
does not enter customer operations.
