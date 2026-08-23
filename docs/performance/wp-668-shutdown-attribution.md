# WP-668 graceful-shutdown attribution

WP-668 is complete as an attribution package. It changes no shutdown order,
deadline, checkpoint rule, failure classification, readiness rule, or worker
scheduling. The production process now emits one additive, redaction-safe V1
receipt containing the graph-local wall time and exactly eight ordered stage
times:

1. admitted service jobs becoming idle;
2. exact-text worker drain;
3. columnar worker drain;
4. projection worker drain;
5. notification closure;
6. commit-coordinator drain;
7. blocking-port drain; and
8. the final validated-prefix checkpoint.

The benchmark parser treats the receipt as optional, so a control server from
before WP-668 remains parseable and reports absent stage evidence. The harness
also records its own complete child-shutdown wall separately. That outer value
is not included in the graph closure ratio because it includes process exit,
stdout delivery, and harness/reaper coordination after the graph is closed.

## Full-dataset cloud receipt

Both hosts ran the same full TicketDesk dataset (19,220 seed commands), one
generated `GetTicket` client, 32 measured calls, four warmups, the synchronous
and asynchronous transport shapes, and four clean daemon generations. N1 and
E2 ran concurrently; each host ran only its own diagnostic.

Times below are microseconds. `Other / residual` is graph wall less columnar
drain and checkpoint; it includes the six small stages and sub-measurement
orchestration between their boundaries.

| Host / generation | Graph wall | Columnar | Checkpoint | Other / residual | Closed |
| --- | ---: | ---: | ---: | ---: | ---: |
| N1 seed generation | 1,049,564 | 175,517 | 870,711 | 3,336 | 99.9995% |
| N1 synchronous read generation | 1,295,081 | 1,082,162 | 210,371 | 2,548 | 99.9995% |
| N1 asynchronous read generation | 226,670 | 6,047 | 218,272 | 2,351 | 99.9969% |
| N1 final generation | 257,557 | 40,925 | 214,121 | 2,511 | 99.9977% |
| E2 seed generation | 653,124 | 76,266 | 570,067 | 6,791 | 99.9988% |
| E2 synchronous read generation | 254,278 | 92,666 | 156,695 | 4,917 | 99.9957% |
| E2 asynchronous read generation | 254,743 | 89,324 | 160,660 | 4,759 | 99.9969% |
| E2 final generation | 321,651 | 153,754 | 162,944 | 4,953 | 99.9975% |

The receipt closes well above the 95% exit gate in every generation. The six
non-dominant stages together account for at most 1.94% of graph wall. There is
no evidence for changing service-job, exact-text, projection, notification,
coordinator, or blocking-port shutdown behavior.

## Finding

The delay has two owners rather than one:

- The final validated-prefix checkpoint is the stable owner. It consumes
  50.7% to 96.3% of graph wall across the eight cloud generations and remains
  material even after read-only generations.
- Columnar drain is the variable owner. It consumes 2.7% to 83.6% on N1 and
  11.7% to 47.8% on E2, depending on how much background replay remains at the
  shutdown boundary.

WP-667 already tested and rejected five bounded columnar-turn schedules: each
violated at least one five-percent lifecycle, request-latency, throughput, or
resource gate. WP-668 therefore does not reopen worker slicing merely because
columnar drain is visible. The next candidate must first decompose the
validated-prefix checkpoint and prove whether its stable cost is pay-per-use
revalidation, durable encoding, redb apply, or the durability fence. It must
not weaken ADR-0019's non-fatal final-checkpoint semantics or move work into an
acknowledged-write path without a predeclared interactive-tail guard.

The harness-observed wall exceeds graph wall by 115 to 306 milliseconds on the
cloud receipts. That is an explicitly named outer boundary—transport/process
exit, evidence delivery, and reaper observation—not unattributed graph work.
It is not the owner selected for the next package.

## Receipt custody

Value-bearing JSON and logs remain outside the repository:

- N1 JSON: `/home/user/tmp/wp668-n1-full-c1.json`, SHA-256
  `7ccc2d83785137d3979e9f8c5e4119a115030bbf778410707579776655efdb3b`
- N1 log: `/home/user/tmp/wp668-n1-full-c1.log`, SHA-256
  `263da9b50c45eecd89ced57fab1c315379ef52fb5d79b22d69e0e456ed192938`
- E2 JSON: `/home/user/tmp/wp668-e2-full-c1.json`, SHA-256
  `2a0d66ff0fcb2ba06334f87eb80904d47b33aca73233319780d913a32f92734c`
- E2 log: `/home/user/tmp/wp668-e2-full-c1.log`, SHA-256
  `eb279f925892309ca1e30bd1a473a68779fae6fca78cbb26950dd71dddbb978b`

The shared source archive used on both hosts was
`/home/user/tmp/wp668-source-4d6f7f7e.tar.gz`, SHA-256
`8153e66a72b0deaafcf04f3fdcdefc2a06ef50ccefb00ac56062d10a7cb9b389`.
The N1 and E2 server binary hashes were respectively
`32d3f1a1f89802c8143572dca71936335d3d6b6546f76d75dcdbf6f81e418c5f`
and `59796cf7583e91d5a3183b04f7640f36d858a0d463f94de5bf04da0562ba3c42`;
the diagnostic binary hash was
`2d19626277e1874cb0d5eb63288586112fd514451e3965efc78374e734f712d0`
on both hosts.
