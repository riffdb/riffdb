# WP-665 serial gRPC session closure

WP-665 is complete without production activation. A private
`requested_max_in_flight == 1` strategy removed the generic client response
router and server operation collection while preserving application-session
protocol V1. The first N1 generation reduced exact generated `GetTicket`
outside-service time by only 31.1 percent against the 50 percent minimum in
ADR-0140. The reject-first matrix stopped immediately. The client and server
specialization, diagnostic selector, architecture pins, lifecycle test, and
prospective pool surface were removed before commit. Unary gRPC remains the
only generated-operation default, the existing optional multiplexed session
is unchanged, and `PERF-018` is unchanged.

## Candidate identity

The candidate was staged on accepted-ADR base `d102916d` and deliberately was
not committed after its reject-first failure. The ordered SHA-256 census of
its nine touched implementation, test, and diagnostic files was:

`261fd584e33535d65f840d0431367b80d52085ba345f163367f0a5468698b162`

N1 built these exact candidate sources into:

- `riffdbd`:
  `eca93935cb3398e487869cf4ac7f2bc42eaa90fb0856b5565d7463090591695c`
- `riffdb-client-transport-diagnostic`:
  `8baa9d4e6911fff24ab3a6ab33e1850ae41609a0ff4e746aedbd71f27cec3173`

The source kept `ApplicationSessionService.Open`, every Protobuf message, and
`APPLICATION_SESSION_PROTOCOL_V1` byte-identical. Max-one selected the private
strategy; values 2 through 128 retained ADR-0127's implementation.

## Reject-first result

The completed N1 generation used the full TicketDesk dataset, one persistent
caller, 32 warmups, 500 measured exact generated `GetTicket` operations, and
fresh daemon generations between unary, generic-session, and serial-session
cells. API-neutral service time is the matched sum of the existing twelve
read-stage counters, including warmups. Outside-service time is the paired
asynchronous caller mean less that matched service mean.

| Shape | Caller mean | Service mean | Outside-service mean | Reduction from unary |
| --- | ---: | ---: | ---: | ---: |
| Unary gRPC | 601.267 us | 147.276 us | 453.991 us | reference |
| Generic session V1 | 461.664 us | 125.577 us | 336.087 us | 25.97% |
| Serial session V1 candidate | 433.757 us | 121.105 us | 312.652 us | **31.13%** |

Serial ownership improved complete caller time by 27.86 percent from unary
and improved outside-service time by only 6.97 percent from the retained
generic session. The accepted mechanics minimum was a 50 percent reduction
from unary. This first cloud generation is therefore conclusive rejection;
setup, further counterbalanced N1/E2 generations, verified TLS, the public
ratio matrix, and a pool cannot be interpreted as passed or needed.

A non-gating workstation screening sample had already pointed the same way:
118.258 us unary, 83.627 us generic session, and 92.740 us serial session for
the paired asynchronous call. It did not substitute for the required N1
falsifier.

## Receipt custody

Value-bearing JSON remains outside the repository:

- N1 generation one: `/home/kevin/tmp/wp665-n1-g1.json`, SHA-256
  `2b0ccec46cb4b10e1815f23f6c11bcfbf6393f78fa17d58f9d8b74641af0682c`
- workstation screening: `/home/kevin/tmp/wp665-local-smoke.json`, SHA-256
  `3f7284e123b48859ea5fb5f6e779c658e77a8e2611e1f1e1b3b27c03f943fa29`

## Decision

The experiment narrowed the residual: response routing is not the dominant
remaining unary/session cost on N1. Removing the general router recovers only
a small increment beyond the already shipped optional multiplexed session,
far below the gain required to make the public alpha ratios pass. A fixed
serial pool would multiply setup and lifecycle state without changing that
per-operation mechanism, so it was not built.

No max-one selector, serial lane, pool, benchmark flag, public API, target-
language implementation, or customer documentation remains. WP-623 stays
open. Any later transport proposal must identify a larger measured mechanism
and receive its own accepted decision rather than weakening ADR-0140's failed
threshold.
