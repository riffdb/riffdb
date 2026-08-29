# Write-path decomposition

Where one durable command's latency goes, measured rather than reasoned.

## Read this first: build profile

**Every number here is a release build.** An earlier revision of this document
measured a **debug** `riffdbd` and compared it against release Postgres, because
`scripts/riffdb-dev` defaults to `profile=debug` and nobody checked. The
conclusions inverted when it was corrected:

| | debug | release |
|---|---:|---:|
| OpenFGA conformance suite | 38.12 s | **15.84 s** |
| versus Postgres 12.53 s | 3.04× | **1.26×** |
| One single-tuple write | 6099 µs | **2162 µs** |
| Writer CPU per command | 2372 µs | **385 µs** |
| `exec_evaluate` (the interpreter) | 646 µs | **118 µs** |
| Journal frame encode | 393 µs | **8 µs** |
| Driver-host hop | 758 µs | **110 µs** |
| fsync | 1231 µs | **1213 µs** |

fsync is the only stage that did not move, because it is real I/O. Everything
else was 6–50× inflated. A debug profile does not scale a system uniformly: it
inflates CPU and leaves I/O alone, so it does not merely shift the numbers, it
**reorders them**. Confirm the profile before drawing any conclusion from a
measurement in this repository.

## Method

500 single-tuple OpenFGA tuple writes against a live release service on the
workstation, with `RIFFDB_WRITER_BATCH_DIAGNOSTICS=1` and
`RIFFDB_COMMAND_SERVICE_DIAGNOSTICS=1`. Client latency is measured in the
adapter; stage numbers come from the censuses the daemon emits.

Single-tuple is the pessimal shape, chosen deliberately: the cost is dominated
by per-command work, so one tuple per command maximises its share. Real OpenFGA
batches up to 100 per write.

## One single-tuple write: 2162 µs at the client

| component | µs | share |
|---|---:|---:|
| **fsync** | 1213 | **56.1%** |
| Writer CPU | 385 | 17.8% |
| Coordinator queue and scheduling | 265 | 12.3% |
| Transport, auth, gRPC, driver host | 232 | 10.7% |
| Service work | 46 | 2.1% |
| Publication work | 21 | 1.0% |
| Journal frame encode | 8 | 0.4% |

Writer CPU decomposes as `exec_evaluate` 118, `exec_apply` 72,
`exec_stage_group` 60, `exec_detach` 56, `exec_prepare_detached` 24,
`exec_seal` 22.

The service layer is 46 µs — prepare 7, snapshot wait 16, normalize 8, input
facts 6, audit begin 4, release 4, admit 1. It is not where time goes.

## Durability is the write path

At 56% of a single-tuple write, fsync dominates everything else combined. That
is the safety work, it is real I/O, and it does not respond to CPU optimisation.
It responds to **amortisation**, and group commit already delivers that: under
concurrent load the flush census records **7.12 commands per flush**.

The interpreter — the largest attackable block under the debug measurement, at
27% of writer CPU — is **118 µs, 5.5% of the write** in release. Compiling the
sealed plan is not the lever the debug numbers suggested.

## Concurrency

Same workload, release, 40 writes per client:

| clients | one store | scaling | distinct stores | scaling |
|---:|---:|---:|---:|---:|
| 1 | 448 ops/s | 1.0× | 471 ops/s | 1.0× |
| 4 | 1485 | 3.3× | 1535 | 3.3× |
| 8 | 2353 | 5.2× | 2543 | 5.4× |
| 16 | 2392 | 5.3× | 3742 | 7.9× |
| 32 | 3251 | **7.2×** | 3259 | 6.9× |

Writes to one store and writes spread across distinct stores scale the same.
The `Tuples` aggregate declares `conflict_key (store_id)`, and the concern that
this would serialise same-store writes is **refuted**: the ratio sits at ~1.0
throughout.

## The driver host

The Go adapter reaches the database through `riffdb-driverd`:

```
Go client ──(unix socket, JSON)──▶ riffdb-driverd ──(gRPC/TLS, pooled)──▶ riffdbd
```

Everything is persistent — the Go client dials once and multiplexes by request
ID; driverd pools gRPC connections. No handshake per call.

Measured by running the same `ApplyTupleMutations` from a release Rust client
that connects directly: **2052 µs direct versus 2162 µs through the host**, so
the hop costs **110 µs, 5.1%** of the write. The debug measurement put it at
758 µs and 12.4%, which overstated it 6.9×.

## Reading these numbers safely

Four diagnostic residuals in this codebase have pointed the wrong way:

- `exec_alternate_path` looks like a residual. It is a **parent** — the loop it
  wraps calls the compatible-group driver, so `exec_evaluate`, `exec_seal`, and
  `exec_apply` are charged inside it and double counted.
- `flush_us` is a bit-for-bit alias of `commit_us`; it is not a flush
  measurement.
- `COMMAND_PUBLICATION_RESIDENCE_MICROS` is **not** CPU. It is
  registration-to-publication residence and overlaps the journal lane.
  `COMMAND_PUBLICATION_WORK_MICROS` — 21 µs — is the work.
- "Service/transport" as an aggregate is mostly transport. The service layer is
  46 µs of it.

Check whether a stage is a parent, an alias, or a duration before targeting it —
and check the build profile before targeting anything.
