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

## Open

The 1751 µs service/transport block is the largest single component and has no
stage census. It should be decomposed before anything is optimised against it.
