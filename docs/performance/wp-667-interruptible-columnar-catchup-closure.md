# WP-667 interruptible columnar catch-up closure

WP-667 is complete without a production change. The investigation proved that
the columnar engine can retain one frozen authoritative scan across bounded
page turns without exposing an incomplete snapshot, but every worker schedule
tested missed at least one predeclared five-percent cloud gate. All candidate
engine APIs, scan-continuation state, worker scheduling, and tests were removed.

## Semantic candidate

The private candidate added a positive page-turn budget capped at eight
existing 64-record commit pages. A 513-commit deterministic history established
the safety boundary:

- the first eight-page turn processed through commit 512 and remained
  `Building` at `BeforeFirst`;
- checkpointing that unpublished working state returned the existing typed
  `HoldbackActive` refusal;
- the next turn resumed the original frozen upper fence, reached `ExactEnd`,
  and published the same logical rows and frontier as unbounded catch-up;
- checkpoint/reopen preserved that completed state; and
- the worker checked its stop token after every turn and never entered the
  25-millisecond poll while a frozen prefix remained.

The candidate did not alter authoritative data, query freshness,
authorization, retention, durable formats, or application interfaces. Budget
exhaustion alone never published, notified, checkpointed, or advanced the
durable frontier. Existing maximal-safe-prefix publication for supersession
holdback remained unchanged.

## Reject-first cloud matrix

The exact pre-WP-667 control server was SHA-256
`406de8ac6aa2449cf36a8e1293e3ac2a1c1e181bb56005c3965adc3842b3ad19`.
Every arm used the same full TicketDesk dataset, diagnostic binary, one client,
32 warmups, 500 synchronous generated `GetTicket` calls, 500 asynchronous calls,
and four clean daemon generations. N1 and E2 ran concurrently but each host ran
its own arms sequentially. The whole-process wall includes install/deploy/seed,
startup validation, reads, derived catch-up, and every clean shutdown; it is a
strict lifecycle guard, not a columnar-only throughput measure.

| Worker schedule | N1 result | E2 result | Decision |
| --- | --- | --- | --- |
| Eight pages, outer pass per turn | request means within 3.0%; wall **+9.88%** | wall -8.73%; no request/memory regression | Reject: N1 wall |
| One page, outer pass per turn | wall -1.01%; request means **+7.64% / +9.78%** | wall +1.72%; request means improved | Reject: N1 reads |
| Four pages, outer pass per turn | wall -5.54%; request means within 1.8% | request means improved; wall **+8.28%** | Reject: E2 wall |
| Two pages, outer pass per turn | wall +2.31%; sync mean **+5.76%**, async PSS **+6.25%** | wall -0.38%; async PSS **+5.18%** | Reject: request/resource gates |
| One page, setup retained across turns | sync/async means within 1.2%; wall **+5.40%** | wall +4.47%; async mean **+15.61%**, async PSS **+10.61%** | Reject: N1 lifecycle and E2 reads/resources |

The final structural variant removed the repeated synchronization, vector
maintenance, head capture, and engine enumeration paid by the earlier
one-page schedule. It released and reacquired the engine lock at every page and
checked shutdown between pages while uninterrupted catch-up retained one outer
pass. That eliminated the obvious orchestration tax, but it still failed on
the paired E2 host and narrowly failed N1 whole-process wall. No tuning value
satisfied both profiles.

## Receipt custody

Value-bearing JSON remains outside the repository. The paired control receipts
and final structural candidate are:

- N1 control: `/home/kevin/tmp/wp667-control-n1.json`, SHA-256
  `900eefa24b8cac27c159f35696493c588aed0ce60b3ef492f78a8bb069699fd5`
- E2 control: `/home/kevin/tmp/wp667-control-e2.json`, SHA-256
  `87bfa8ae613cabc07b723c49d0e76c2420b9c3add5df0f2b23571da8d7a5386b`
- N1 final candidate: `/home/kevin/tmp/wp667-final-n1.json`, SHA-256
  `bd6a64cb896f1ba270c1dd278a1d7f1dfbe91d85b2a852e899adc4c477433479`
- E2 final candidate: `/home/kevin/tmp/wp667-final-e2.json`, SHA-256
  `5166545430dd1c7e26db74e372dac89c79fee9844e2b2f6e9872f86a7f675b3e`

The intermediate one-, two-, four-, and eight-page receipts remain under the
same host-local `/home/kevin/tmp/wp667-*` namespace. Candidate server hashes
were recorded with each run; none is a release artifact.

## Decision

The reported shutdown delay is not safely solved by slicing ordinary columnar
commit replay alone. The evidence suggests either another earlier shutdown
owner contributes materially or cloud catch-up contention is too variable for
this scheduling-only design. A follow-up must first add fixed-cardinality
per-worker shutdown-stage attribution across exact-text, columnar, projection,
coordinator, blocking-port drain, and the final validated-prefix checkpoint.
It may then bound the measured owner rather than infer ownership from total
process wall.

The pre-WP-667 unbounded columnar apply behavior remains active. WP-623 and
PERF-018 are unchanged.
