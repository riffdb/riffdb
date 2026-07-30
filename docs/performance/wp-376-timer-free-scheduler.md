# WP-376 timer-free self-clocking scheduler

Status: implementation and measured evidence.

## Finding

The production coordinator and its separate historical-idempotency reader each
used a 200-microsecond `tokio::time::timeout_at` grouping window. Tokio 1.52's
time driver rounds sub-millisecond deadlines up to millisecond ticks. A lone
mutation crossed both windows and therefore paid roughly two milliseconds of
fixed scheduling delay before doing command work.

The idempotency window was not an authoritative writer window. ADR-0060 permits
bounded grouped historical selection, but does not require a second timed wait.
The writer window is also optional: the accepted rule allows the scheduler to
wait for compatible work, rather than requiring every idle singleton to wait.

## Product rule

Low-load work dispatches immediately. Saturated grouping is self-clocking:
commands arriving while the prior durable transaction is in progress
accumulate in the bounded channel and are drained when the writer is ready.

- The historical-idempotency thread blocks only for its first item and then
  drains immediately available items up to the existing bound.
- The writer drains the complete immediately available receiver into its
  ordered local queue, applies the existing barrier-aware selection, and
  dispatches.
- No busy-spin, sleep, Tokio timer, extra writer, reordered barrier, or changed
  acknowledgement point is introduced.
- `QueueDrained` replaces the misleading `WindowElapsed` telemetry reason.

## Same-run evidence

The first full TicketDesk run after removing the timer parks recorded:

| Scenario | PostgreSQL p50 | RiffDB p50 | RiffDB/PostgreSQL |
| --- | ---: | ---: | ---: |
| `create_comment` | 1.073 ms | 0.548 ms | 0.51x |
| `close_ticket_with_comment` | 2.285 ms | 0.544 ms | 0.24x |
| `swap_member_roles` | 1.182 ms | 0.483 ms | 0.41x |
| `open_ticket_with_labels` | 1.254 ms | 0.641 ms | 0.51x |

Before the correction, representative RiffDB unary writes were uniformly
approximately 2.6 to 2.8 milliseconds. The fixed delay disappeared while the
full 15,160-command seed remained 2.127 seconds versus PostgreSQL's 0.661
seconds. That separation confirms the timer regression affected low-load
latency and that the remaining seed gap is a distinct CPU-path problem owned by
WP-378 and the final WP-379 gate.

The same seed retained saturated grouping: 212 physical completion groups
contained 64 commands.

## Safety evidence

Architecture and coordinator tests freeze:

- absence of Tokio grouping timers and enabled time drivers;
- bounded `blocking_recv` plus `try_recv` idempotency drain;
- bounded writer receiver drain and `QueueDrained` dispatch;
- exact 64-item group behavior;
- stable observation deferral without crossing hard barriers;
- shutdown, fencing, cancellation, and unknown-status behavior.

No durable format, command graph, authorization check, transaction validation,
sequence order, idempotency identity, audit record, or response-release rule
changed.
