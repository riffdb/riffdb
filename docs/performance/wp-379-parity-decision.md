# WP-379 renewed application parity decision

Decision: **not eligible**. WP-370 must not start.

The same-run full TicketDesk comparison under the standard durability profile
completed with RiffDB at 1.503 seconds for 15,160 commands and PostgreSQL at
0.647 seconds. The 2.324 ratio misses the `PERF-008` seed limit of 1.10.

Every representative mutation p50 passed and beat PostgreSQL:

| Scenario | PostgreSQL | RiffDB | RiffDB / PostgreSQL |
|---|---:|---:|---:|
| `create_comment` | 1.091 ms | 0.492 ms | 0.45 |
| `close_ticket_with_comment` | 1.261 ms | 0.553 ms | 0.44 |
| `swap_member_roles` | 2.092 ms | 0.452 ms | 0.22 |
| `open_ticket_with_labels` | 1.225 ms | 0.560 ms | 0.46 |

RiffDB named reads remained fast in absolute terms at 0.20–0.28 ms, but warm
prepared PostgreSQL reads were 0.04–0.17 ms. Point and list ratios therefore
remain above the unchanged 1.10 threshold. The 1/8/32/64-client evidence also
completed, and its assertion command correctly returned nonzero.

The immutable decision and exact generated reports are in
`release/evidence/wp379-parity-decision-v1.json` and its referenced sibling
files. No miss is waived.

The next pass should use the new closed stage events to separate remaining
idempotency-selection, admission, evaluation, validation/encoding/staging,
commit, and publication cost. Read work should focus on application-service
authorization/response construction and the anomalous point-ticket path before
repeating the full gate.
