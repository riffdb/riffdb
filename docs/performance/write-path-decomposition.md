# Write-path decomposition

Where one durable command's latency goes, measured rather than reasoned.

## Method

500 single-tuple OpenFGA tuple writes against a live service on the
workstation, `RIFFDB_WRITER_BATCH_DIAGNOSTICS=1`. Client latency is measured in
the adapter; the stage numbers come from four censuses the daemon already
emits (`riffdb-writer-batch-stages-v1`, `-journal-stages-v1`,
`-flush-census-v1`, `-publication-stages-v1`).

Single-tuple is the pessimal shape and is chosen deliberately: a batch sweep
over the same command shows **~4 ms fixed per command and ~1.22 ms marginal per
tuple**, so one tuple per command maximises the fixed share. At 100 tuples per
command the per-tuple cost falls to 1.26 µs·10³ — below the same host's
Postgres single-write cost.

| batch | total | per tuple |
|---:|---:|---:|
| 1 | 5.55 ms | 5.55 ms |
| 10 | 16.06 ms | 1.61 ms |
| 100 | 126.34 ms | 1.26 ms |

## One single-tuple write: 5822 µs at the client

| component | µs | share | contents |
|---|---:|---:|---|
| Writer CPU | 2372 | 40.7% | evaluate 646, apply 548, seal 327, detach 313, stage_group 222 |
| Journal lane | 1652 | 28.4% | fsync 1231, frame encode 393, journal write 20, queue 8 |
| Publication work | 47 | 0.8% | receipt block 0 — the fence is already durable |
| Service / transport | 1751 | 30.1% | **unmeasured** |

The journal lane and publication residence are the same time observed from two
sides (1652 µs versus 1696 µs), not additive. `RECEIPT_BLOCK` reads zero: by the
time the completion thread reaches the fence it is already durable, so the
fsync overlaps rather than serialising behind the writer.

## Grouped by what the time *is*

| | µs | share |
|---|---:|---:|
| Service path — unmeasured | 1751 | 30.1% |
| Re-derivation of what the compiler already sealed | 1666 | 28.6% |
| Durability | 1578 | 27.1% |
| Apply — real state mutation and index deltas | 548 | 9.4% |

"Re-derivation" is `exec_evaluate` + `exec_detach` + `exec_stage_group` +
`exec_prepare_detached` + journal frame encode: the family ADR-0142 named as
"validation, encoding, and staging". Command evaluation is a tree-walking
interpreter over the IR (`ExpressionEvaluator` over an `ExpressionArena`),
re-run per invocation against a plan whose shape, write set, affected indexes,
and conflict domain the compiler already proved.

## What this changes about ADR-0142

ADR-0142 retired the universal 1.10× unary Postgres gate after proving the gap
could not be closed *by removing safety work*. That holds. It did not test
whether the cost is inherent to the work or inherent to performing it
interpretively per call, and the batch sweep answers that: the same work costs
1.22 ms marginal when amortised across a command and ~4 ms when it is not.

## Reading these numbers safely

Three diagnostic residuals in this codebase have pointed the wrong way:

- `exec_alternate_path` looks like a 37% residual. It is a **parent** — the
  loop it wraps calls the compatible-group driver, so `exec_evaluate`,
  `exec_seal`, and `exec_apply` are charged inside it and double counted.
- `flush_us` is a bit-for-bit alias of `commit_us`; it is not a flush
  measurement.
- `COMMAND_PUBLICATION_RESIDENCE_MICROS` reads 1696 µs/cmd and is **not** CPU.
  It is registration-to-publication residence, and it overlaps the journal
  lane. `COMMAND_PUBLICATION_WORK_MICROS` — 47 µs — is the work.

Check whether a stage is a parent, an alias, or a duration before targeting it.

## The service path, measured

`riffdb-command-service-stages-v1` (`RIFFDB_COMMAND_SERVICE_DIAGNOSTICS=1`)
closes the last gap. Same workload, 500 single-tuple writes, 6099 µs at the
client:

| | µs | share of the write |
|---|---:|---:|
| Transport, auth, gRPC — outside `execute_command` | 1275 | 20.9% |
| `svc_commit_wait` — writer plus journal | 4507 | 73.9% |
| All other service work | 247 | 4.1% |

`svc_commit_wait` decomposes against the writer censuses:

| | µs |
|---|---:|
| Writer CPU | 2372 |
| Journal lane (fsync 1231, frame encode 393) | 1652 |
| Coordinator queue and scheduling | 483 |

The service layer itself is **247 µs, 4.1%** — prepare 35, snapshot wait 88,
normalize 52, input facts 26, audit begin 21, release 24, admit 3, residual 70.
It is not where the time goes, and the earlier "1751 µs service/transport"
figure was mostly transport, not service.

## Complete accounting for one 6099 µs single-tuple write

| component | µs | share |
|---|---:|---:|
| Writer CPU | 2372 | 38.9% |
| Journal lane | 1652 | 27.1% |
| Transport, auth, gRPC | 1275 | 20.9% |
| Coordinator queue and scheduling | 483 | 7.9% |
| Service work | 247 | 4.1% |

Transport at 1275 µs is worth its own note: a raw gRPC round trip on this host
measures about 44 µs, so this is roughly 29× the framing floor. Whatever it is
— TLS, driver marshalling, capability and request-context construction — it is
not gRPC framing, and it is the second largest attackable block after
evaluation.

## Ranked, by what can be attacked without weakening a guarantee

| target | µs | note |
|---|---:|---|
| Re-derivation the compiler already sealed | 1666 | interpreter, detach, staging, frame encode |
| Transport, auth, request framing | 1275 | 29× the gRPC floor; undecomposed |
| Durability | 1578 | fsync is irreducible per commit; amortises only with concurrency |
| Apply — real state mutation | 548 | genuine work |
| Coordinator queue and scheduling | 483 | undecomposed |
| Service layer | 247 | already small |
