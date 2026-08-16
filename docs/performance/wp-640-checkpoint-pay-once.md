# WP-640 checkpoint pay-once apply

## Scope

This increment removes a repeated proof from the live checkpoint path. The
command and service-audit staging paths already construct, normalize, validate,
and encode each ordered `CompositeMutationV1`. While the process remains live,
the checkpoint worker now receives those exact validated mutations alongside
the encoded journal-frame identity and applies them with transaction-current
before-image checks. It no longer decodes, preflights, checksums, hashes, and
reconstructs the same durable frame a second time.

This does not change the journal, checkpoint, or redb durable formats. Startup,
recovery, backup validation, and every externally sourced byte path continue to
decode and validate durable bytes independently. The retained mutation values
carry neither durability nor publication authority and are bounded by the
existing journal-suffix transition and byte limits. Cloned checkpoint batches
share the values through `Arc` rather than duplicating their contents.

## Safety proof

- The live composite stage returns mutations only from the same successful seal
  that proves the encoded frame kind, database, predecessor and covered
  frontiers, transition count, encoded length, previous hash, and frame hash.
- The checkpoint batch rechecks that complete frame identity and the ordered
  frame chain before applying any retained mutation.
- Every mutation still checks its expected before-image hash against the current
  redb write transaction. A mismatch aborts the checkpoint rather than trusting
  the retained value.
- The batch totals must close exactly for application and administration
  frontiers, terminal hash, transition count, command count, audit count, and
  encoded bytes before the transaction can commit.
- Recovery never calls the pay-once apply function. It continues through the
  independent durable-frame decoder and validator.

Automated coverage freezes byte-identical live-versus-decoded composite views,
the exact retained mutation sequence, and transaction-current rejection of a
stale replacement.

## Performance evidence

All results use the full 19,220-command TicketDesk seed, three repetitions, the
same public gRPC path, and the exact `52e15678` baseline on each host. Older
nearby commit receipts remain diagnostic only and are not release comparators.

| Host | Baseline | Candidate | Change |
|---|---:|---:|---:|
| Workstation | 1.946 s | 1.792 s | 7.9% faster |
| GCP N1 | 7.316 s | 6.831 s | 6.6% faster |
| GCP E2 | 6.667 s | 6.500 s | 2.5% faster |

A paired 15-second interactive c32 diagnostic (three repetitions, non-
evidentiary duration) also found no operation-level regression. These short
cells are diagnostic receipts for this increment; they do not satisfy
WP-640's same-run PostgreSQL release-evidence deliverable.

| Host | Metric | Baseline | Candidate | Change |
|---|---|---:|---:|---:|
| Workstation | Throughput | 28,608 ops/s | 31,292 ops/s | 9.4% faster |
| Workstation | `create_comment` p50 | 5.59 ms | 4.98 ms | 10.9% lower |
| Workstation | Seed in the same runs | 2.239 s | 2.082 s | 7.0% faster |
| GCP N1 | Throughput | 7,244 ops/s | 7,482 ops/s | 3.3% faster |
| GCP N1 | `create_comment` p50 | 18.87 ms | 18.53 ms | 1.9% lower |
| GCP N1 | Seed in the same runs | 7.628 s | 7.074 s | 7.3% faster |
| GCP E2 | Throughput | 6,356 ops/s | 7,448 ops/s | 17.2% faster |
| GCP E2 | `create_comment` p50 | 21.32 ms | 17.30 ms | 18.9% lower |
| GCP E2 | Seed in the same runs | 6.879 s | 6.318 s | 8.2% faster |

The exact N1 control was tight at 7,231, 7,222, and 7,279 ops/s. The E2
control's third repetition dropped to 4,776 ops/s while its first two were
7,102 and 7,191 ops/s, so the E2 mean is intentionally reported but must not be
treated as a stable release ratio. Every candidate and control mixed-load cell
reported zero operation errors.

An additional candidate repetition completed at 32,816 ops/s with a clean
post-shutdown inventory. One earlier candidate repetition set completed all
three measured cells with zero operation failures but failed the harness's
post-measurement table-inventory read on its final process. Because the exact
baseline did not reproduce that particular inventory failure, it remains a
disclosed reliability observation rather than being silently classified as a
pass; the additional clean repetition and all six cloud candidate cells bound
it separately from operation correctness.

Receipts:

- `/home/kevin/tmp/wp640-profile-current.json`
- `/home/kevin/tmp/wp640-checkpoint-payonce-seed.json`
- `/home/kevin/tmp/wp640-52e-mixed32-workstation.json`
- `/home/kevin/tmp/wp640-checkpoint-mixed32-workstation.json`
- `/home/kevin/tmp/wp640-checkpoint-mixed32-workstation-recheck.json`
- N1: `/home/kevin/tmp/wp640-52e-baseline-n1.json` and
  `/home/kevin/tmp/wp640-checkpoint-n1.json`
- E2: `/home/kevin/tmp/wp640-52e-baseline-e2-rerun.json` and
  `/home/kevin/tmp/wp640-checkpoint-e2-rerun.json`
- N1 mixed c32: `/home/kevin/tmp/wp640-52e-mixed32-n1.json` and
  `/home/kevin/tmp/wp640-checkpoint-mixed32-n1.json`
- E2 mixed c32: `/home/kevin/tmp/wp640-52e-mixed32-e2.json` and
  `/home/kevin/tmp/wp640-checkpoint-mixed32-e2.json`

The first E2 candidate attempt stopped after nine committed seed commands with
the service fail-closed `AuditUnavailable` terminal. The first exact-baseline
attempt reproduced the same stop after six committed seed commands. Both
failures occurred before the checkpoint start threshold, and both warm
three-repetition reruns completed. The stop is therefore retained as a
separate reliability finding rather than attributed to checkpoint
materialization.

## Rejected sibling candidate

A transient decoded command-segment cache for the columnar consumer improved
the workstation seed by about 1.5% but regressed its adjacent N1 baseline from
about 7.23 s to 7.477 s and produced no independently credible cloud win. It
was removed. This increment retains only the checkpoint handoff subject to the
exact paired cloud comparison above.

## Acceptance plumbing note

The repository's recovery entry point is `./scripts/recovery_full`; the former
WP-640 command named a script that does not exist. The matrix also still
expected the pre-group-commit response label `sync`. Both the unchanged
`52e15678` baseline and this candidate returned the correct current label
`group`, so the assertion is updated without changing runtime behavior.

After that correction, `recovery_full` still stops in a later process-level
maintenance arm because shutdown evidence arrives before the expected ready
line. An instrumented exact-`52e15678` worktree reproduces the same condition;
the checkpoint candidate therefore does not own it and does not weaken the
harness to pass. The package-level redb recovery matrix remains green at all 86
cases. The process-level launcher failure stays explicit until its maintenance
owner repairs the pre-existing readiness sequence.

Requirement coverage: PERF-001, PERF-004, PERF-005, PERF-008, PERF-018.
