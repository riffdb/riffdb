# WP-670 private exclusive-lane verdict

WP-670 is closed without production activation. ADR-0141's private candidate
proved that direct stream ownership materially removes transport overhead, but
it failed the actual complete-unary product gate on the first cloud generation.
The candidate was therefore removed before pool, driver-host, public selector,
compatibility, or release hardening.

This report is short, non-evidentiary diagnostic evidence. It does not qualify
`PERF-018`, change unary gRPC, or authorize WP-671 or WP-672.

## Identity and custody

- Accepted decision baseline: `c68baaf0`.
- Measured private identity: `RDBX-WP670-V3` / ALPN
  `riffdb-private-wp670/3`. Version 3 is distinct from the removed
  `RDBX-DIAG-V2` experiment and therefore satisfies ADR-0141's prohibition on
  reusing rejected bytes.
- The exact measured source content manifest has SHA-256
  `956bad7626629ebcc1f26e52d2fa4e32b8b723b7621028aeddff7ca272c77be9`.
- Reproducibility commit `4ef91ff7` differs from that manifest only by a
  Clippy-required collapse of two nested evidence-map conditions; it does not
  change framing, execution, timing, or evidence arithmetic.
- Removal commit: `801f417a`.
- N1 safe-PostgreSQL report:
  `3c9680dde0fb2ce61a4d618399ee1339b2636e03e4e821647edfc85fc52d24f5`.
- N1 candidate report:
  `dcc32bf3955a180f472740d9ad21afd1a42919340f55e13c73bbfca7a929f514`.
- E2 safe-PostgreSQL report:
  `f00629c9bc3ce3821d955aa6c06f68d4a8017b4394aaf40aed65c3dc309d1bf8`.
- E2 candidate report:
  `9d24e6ab70a978e68c67ff711b5bd054e7f123b7cdc46907ce619ba1789a7553`.

Value-bearing JSON remains outside repository history under
`/home/user/tmp/wp670-verdict-{n1,e2}-loop-1-{pg,rd}.json`.

Both hosts ran 64 measured calls after 16 warmups on the full TicketDesk data
shape. PostgreSQL and RiffDB ran sequentially on the same persistent device.
The candidate used the normal generated TicketDesk facade; seed and projected
board probes remained on public gRPC. No application code authored frames,
parameters, decoding, retries, authorization, freshness, or uncertainty logic.

## Mechanics result

The minimum safe candidate passed strict fragmentation, malformed-magic,
oversize-length, bounded-output, application-session identity, generated
query/command, and architecture-confinement tests. It used the existing
API-neutral application handler and current per-operation authentication,
authorization, policy, freshness, command, durability, and response-release
paths.

The private caller/server ledger closed far inside ADR-0141's five-percent
limit. The largest operation-level error was 0.167 percent on N1 and 0.078
percent on E2. A local cleartext smoke and a verified-rustls smoke both passed
through real `riffdbd`, generated reads, and generated commands.

Against the current-head unary preflight, generated `GetTicket` mean improved:

| Host | Unary mean | Candidate mean | Reduction |
| --- | ---: | ---: | ---: |
| N1 | 886 us | 572 us | 35.48% |
| E2 | 1,803 us | 1,140 us | 36.77% |

The candidate therefore passed the coarse 25-percent point-read falsifier on
both hosts. The public product matrix, not that proxy, decides retention.

## Complete small-operation verdict

The following table reports same-generation p50. `Service / budget` compares
the API-neutral application-service mean with `1.10 * PostgreSQL p50`; values
above 1.0 prove that even perfect remaining transport cannot fit the gate.

| Host | Scenario | PG p50 | Candidate p50 | Ratio | Service / budget |
| --- | --- | ---: | ---: | ---: | ---: |
| N1 | `point_get_ticket` | 0.409 ms | 0.565 ms | 1.379x | 0.622x |
| N1 | `point_get_user` | 0.293 ms | 0.445 ms | 1.518x | 0.675x |
| N1 | `list_tickets_by_project_status` | 0.341 ms | 0.686 ms | 2.012x | **1.023x** |
| N1 | `list_open_tickets_for_assignee` | 0.440 ms | 0.953 ms | 2.167x | **1.133x** |
| N1 | `list_comments_for_ticket` | 0.337 ms | 0.533 ms | 1.583x | 0.716x |
| N1 | `list_project_members` | 0.307 ms | 0.513 ms | 1.670x | 0.805x |
| N1 | `ticket_detail_page` | 0.550 ms | 0.716 ms | 1.301x | 0.712x |
| N1 | `create_comment` | 2.773 ms | 3.771 ms | 1.360x | **1.101x** |
| N1 | `close_ticket_with_comment` | 2.748 ms | 3.594 ms | 1.308x | **1.062x** |
| N1 | `swap_member_roles` | 2.440 ms | 3.149 ms | 1.290x | **1.041x** |
| N1 | `open_ticket_with_labels` | 2.581 ms | 3.917 ms | 1.517x | **1.230x** |
| E2 | `point_get_ticket` | 1.051 ms | 1.140 ms | **1.085x** | 0.434x |
| E2 | `point_get_user` | 0.609 ms | 1.025 ms | 1.683x | 0.678x |
| E2 | `list_tickets_by_project_status` | 0.726 ms | 1.316 ms | 1.814x | 0.822x |
| E2 | `list_open_tickets_for_assignee` | 0.730 ms | 1.688 ms | 2.313x | **1.097x** |
| E2 | `list_comments_for_ticket` | 0.549 ms | 1.110 ms | 2.022x | 0.898x |
| E2 | `list_project_members` | 0.467 ms | 1.105 ms | 2.367x | **1.057x** |
| E2 | `ticket_detail_page` | 1.039 ms | 1.338 ms | 1.287x | 0.656x |
| E2 | `create_comment` | 6.293 ms | 6.263 ms | **0.995x** | 0.794x |
| E2 | `close_ticket_with_comment` | 5.580 ms | 5.837 ms | **1.046x** | 0.837x |
| E2 | `swap_member_roles` | 5.255 ms | 5.339 ms | **1.016x** | 0.793x |
| E2 | `open_ticket_with_labels` | 6.233 ms | 6.226 ms | **0.999x** | 0.796x |

N1 passed zero of seven ordinary reads and zero of four commands. E2 passed
one of seven ordinary reads and all four commands. Several misses are already
service-floor-owned. The remaining misses with an arithmetically viable service
floor still fail the accepted complete-operation threshold and do not justify
hardening another transport.

`board_page_450` remained separately unfavorable at 1.560x on N1 and 1.574x
on E2. It was not used to reject the small-operation candidate because
ADR-0141 already assigns large-result work separately.

The N1 seed was 3.70x PostgreSQL and the E2 seed was 2.37x PostgreSQL, both
inside the amended 5.0x ceiling. All scenario correctness checks passed.

## Decision

ADR-0141 requires immediate removal when any representative small read or
command remains above 1.10x, and independently when a measured service floor
cannot fit the PostgreSQL budget. Both conditions occurred in the first
loopback generation. Running two more loopback and three TLS generations could
not turn that generation into the required every-generation pass, so those
runs were deliberately not performed.

The private listener, frame crate, client selection, app-baseline switch, and
diagnostic identity were removed. Unary gRPC remains the only release-selected
application transport and `PERF-018` is unchanged. WP-671 and WP-672 are not
authorized by WP-670.

The next performance work must begin inside the API-neutral service/result
path, with separate read/list/detail and command ledgers. It must not revive a
transport candidate, cache mutable authority, weaken safe points, or treat the
large-result miss as transport-owned.
