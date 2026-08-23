# WP-673 small-operation service-floor attribution

WP-673 is closed without an implementation candidate. Exact process-isolated
N1 and E2 ledgers show that no single API-neutral read or command family can
move every unfavorable small operation to the unchanged `1.10x` PostgreSQL
threshold. This is diagnostic evidence, not `PERF-018` release evidence and not
an amendment to the selected unary gRPC product surface.

## Measurement identity and integrity

- Source revision: `897c605f2c54a312ef42629be56028599d57ba8b`.
- Hosts: N1 `bench-host-n1` and E2 `bench-host-e2`, eight vCPUs each.
- Shape: full TicketDesk data, safe-application PostgreSQL, 16 warmups, 64
  measured operations, one repetition, and one frozen generated scenario per
  process generation.
- The RiffDB daemon was restarted after seed, projection preparation, and
  warmup. Each read process therefore contains exactly 64 query executions;
  each command process contains exactly 64 original durable singleton writes.
- All 22 paired reports completed without an operation or correctness failure.
  Every report contains exactly one identically named scenario for both
  backends and 64 samples per backend.
- The sorted SHA-256 manifest of the 22 out-of-tree JSON reports has digest
  `4758fcca96df604e678e17083fb6908c99620d3b7e18f57f78c740a3a5c860f7`.
  Value-bearing reports remain outside repository history under
  `/home/user/tmp/wp673-service-ledgers-{n1,e2}/`.

The private selector accepts only the seven ordinary reads and four existing
commands. It does not add a server operation, application selector, transaction
control, cache control, or public transport shape. Normal comparator ordering
and topology are unchanged.

## Public same-run result

All values below are p50 milliseconds. Ratio is RiffDB divided by
safe-application PostgreSQL.

| Host | Scenario | PostgreSQL | RiffDB | Ratio |
| --- | --- | ---: | ---: | ---: |
| N1 | `point_get_ticket` | 0.250 | 0.683 | 2.73x |
| N1 | `point_get_user` | 0.244 | 0.594 | 2.44x |
| N1 | `list_tickets_by_project_status` | 0.298 | 0.867 | 2.91x |
| N1 | `list_open_tickets_for_assignee` | 0.395 | 1.103 | 2.79x |
| N1 | `list_comments_for_ticket` | 0.270 | 0.666 | 2.46x |
| N1 | `list_project_members` | 0.526 | 0.776 | 1.48x |
| N1 | `ticket_detail_page` | 0.487 | 0.908 | 1.87x |
| N1 | `create_comment` | 3.014 | 4.252 | 1.41x |
| N1 | `close_ticket_with_comment` | 3.066 | 3.747 | 1.22x |
| N1 | `swap_member_roles` | 2.995 | 3.914 | 1.31x |
| N1 | `open_ticket_with_labels` | 4.050 | 4.786 | 1.18x |
| E2 | `point_get_ticket` | 0.308 | 1.082 | 3.51x |
| E2 | `point_get_user` | 0.317 | 0.910 | 2.87x |
| E2 | `list_tickets_by_project_status` | 0.377 | 1.072 | 2.85x |
| E2 | `list_open_tickets_for_assignee` | 0.894 | 2.158 | 2.41x |
| E2 | `list_comments_for_ticket` | 0.329 | 1.312 | 3.99x |
| E2 | `list_project_members` | 0.428 | 2.093 | 4.89x |
| E2 | `ticket_detail_page` | 3.684 | 1.677 | 0.46x |
| E2 | `create_comment` | 5.786 | 5.490 | 0.95x |
| E2 | `close_ticket_with_comment` | 6.847 | 7.332 | 1.07x |
| E2 | `swap_member_roles` | 4.023 | 6.543 | 1.63x |
| E2 | `open_ticket_with_labels` | 4.295 | 7.286 | 1.70x |

The isolated result is intentionally less forgiving than a mixed run. It
preserves the complete small-operation problem rather than allowing favorable
large reads or batching to hide it.

## Read ledgers

The table is a closed mean ledger in microseconds. `Service` is the sum of all
12 fixed service stages. `Caller remainder` is client mean minus that sum and
contains response release, HTTP/2/tonic scheduling, client conversion, and the
caller wait not charged to a service stage. `Plan` and `execute` are included
inside `Service` and shown to identify the largest internal families.

| Host | Scenario | Client mean | Service | Caller remainder | Plan | Execute |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| N1 | `point_get_ticket` | 955 | 391 | 564 | 247 | 52 |
| N1 | `point_get_user` | 826 | 325 | 501 | 199 | 45 |
| N1 | `list_tickets_by_project_status` | 1,134 | 480 | 654 | 206 | 159 |
| N1 | `list_open_tickets_for_assignee` | 1,333 | 631 | 702 | 199 | 297 |
| N1 | `list_comments_for_ticket` | 901 | 384 | 516 | 200 | 97 |
| N1 | `list_project_members` | 1,030 | 421 | 610 | 209 | 112 |
| N1 | `ticket_detail_page` | 1,135 | 533 | 602 | 200 | 214 |
| E2 | `point_get_ticket` | 1,281 | 380 | 901 | 136 | 92 |
| E2 | `point_get_user` | 1,070 | 335 | 736 | 122 | 80 |
| E2 | `list_tickets_by_project_status` | 1,236 | 414 | 822 | 119 | 162 |
| E2 | `list_open_tickets_for_assignee` | 2,373 | 818 | 1,555 | 129 | 447 |
| E2 | `list_comments_for_ticket` | 1,467 | 470 | 997 | 134 | 173 |
| E2 | `list_project_members` | 2,455 | 777 | 1,678 | 200 | 297 |
| E2 | `ticket_detail_page` | 1,780 | 669 | 1,111 | 142 | 316 |

The apparently large mean plan cost is a single process-cold lookup, not a hot
path candidate. Its steady p50 upper bucket is 2 microseconds on all N1 reads
and 2–16 microseconds on E2. The 14 storage-query substages close against the
`execute` stage within 0.98–5.89 percent. Their main components are snapshot
capture, indexed lookup, durable-record verification/reconstruction, row policy
and materialization, and exclusive bounded-program drive; no hidden storage
family remains.

## Command ledgers

These are also per-operation means in microseconds. The five service stages sum
to `Service`; `Caller remainder` closes it against the client mean. The three
rightmost columns decompose `coordinator_await`: the five command stages,
ordered completion through the command's own durable fence/publication, and the
remaining coordinator/channel gap. Those three columns close within the await
stage by construction and preserve the measured 15.5–17.2 percent coordinator
gap instead of assigning it to a nearby stage.

| Host | Scenario | Client | Service | Caller rem. | Prepare | Await | Command stages | Completion | Await gap |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| N1 | `create_comment` | 6,145 | 5,130 | 1,016 | 432 | 4,622 | 2,584 | 1,311 | 727 |
| N1 | `close_ticket_with_comment` | 5,484 | 4,615 | 869 | 458 | 4,086 | 2,385 | 1,068 | 633 |
| N1 | `swap_member_roles` | 5,722 | 4,764 | 958 | 416 | 4,272 | 2,365 | 1,232 | 675 |
| N1 | `open_ticket_with_labels` | 6,543 | 5,517 | 1,027 | 460 | 4,969 | 2,769 | 1,385 | 814 |
| E2 | `create_comment` | 7,085 | 5,549 | 1,535 | 471 | 4,961 | 2,743 | 1,382 | 835 |
| E2 | `close_ticket_with_comment` | 9,237 | 7,060 | 2,177 | 631 | 6,277 | 3,400 | 1,795 | 1,082 |
| E2 | `swap_member_roles` | 8,276 | 6,394 | 1,883 | 592 | 5,664 | 2,955 | 1,740 | 969 |
| E2 | `open_ticket_with_labels` | 8,925 | 7,132 | 1,793 | 580 | 6,421 | 3,610 | 1,741 | 1,070 |

Every command was a singleton group, so the ledger measures unary latency rather
than throughput grouping. Within command stages, validation/encoding/staging is
the largest family (1,875–2,565 microseconds), followed by deterministic
evaluation (425–900 microseconds). Ordered completion includes the measured
0.80–1.29 millisecond sync and current publication work; none may be removed
from an acknowledged durable command.

## Amdahl decision

For reads, the table compares the public p50 reduction required to reach
`1.10 * PostgreSQL p50` with the complete measured `execute` family. The
required multiple is greater than one for every failing read: even deleting
query execution entirely cannot reach the product threshold.

| Host | Best/worst required multiple of complete execute |
| --- | ---: |
| N1 | 1.74x (`ticket_detail_page`) to 7.90x (`point_get_ticket`) |
| E2 | 2.63x (`list_open_tickets_for_assignee`) to 8.06x (`point_get_ticket`) |

E2 `ticket_detail_page` already passes and is excluded. Plan lookup is not an
alternative: its hot p50 is at most 16 microseconds. The dominant remainder is
outside the API-neutral execution family, while WP-670 already proved and then
removed a private transport replacement because the complete scenario set
still failed. WP-673 cannot honestly revive it as a service optimization.

For commands, N1 would need 155–452 microseconds per millisecond of the largest
validation/encoding/staging family removed, so a material duplicate could have
helped there. The cross-host gate rejects that conclusion: E2
`swap_member_roles` needs 2,118 of its measured 2,127 microseconds removed
(99.57 percent), and `open_ticket_with_labels` needs 2,562 of 2,565 microseconds
removed (99.85 percent). This stage contains required validation, canonical
encoding, final apply, and journal staging; eliminating effectively all of it
is neither an optimization nor compatible with the accepted guarantees. The
WP-640 mechanics experiment already rejected moving this proof work into the
prepared-command pipeline on cloud.

There is therefore no single cross-host movable family with an Amdahl bound
that fits all affected scenarios. WP-673 authorizes no production candidate.

## Closure

The measurements do not support further serial-lane squeezing, query/storage
micro-optimization, or another transport experiment. Unary gRPC, durability,
authorization safe points, query boundedness, and `PERF-018` remain unchanged.
The universal `1.10x` small-operation threshold returns to the maintainer as a
product-rule decision: retain it and accept that alpha remains blocked pending
a broader architecture change, or replace it with a gate that reflects the
measured heterogeneous-cloud service and protocol floor.

