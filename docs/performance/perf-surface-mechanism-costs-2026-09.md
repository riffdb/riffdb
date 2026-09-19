# Per-mechanism write cost: projections, text-key and tokenized indexes

First measurements of three subsystems that had no benchmark coverage anywhere
in the repository. Taken on the C3D bench host with `benchmarks/perf-surface`,
2,000 published documents per variant, revision `2c5d7aa25`.

Each variant is rendered from one template and differs only in the clause under
test, so a difference between two rows is the mechanism rather than two
contracts that differ in many ways. Every variant emits the event a projection
would consume, so the projection row is not charged for event emission.

## One client

Every command is its own commit here (1.0 documents per commit in all five
variants), so byte figures carry no grouping confound.

| variant | bytes/doc | delta | writer busy us/doc | delta | docs/s |
|---|---|---|---|---|---|
| base | 7,120.8 | | 497.5 | | 728 |
| projection | 7,120.8 | **-0.0** | 1,130.5 | **+633.0** | 487 |
| text_key | 8,273.6 | **+1,152.8** | 516.8 | +19.3 | 712 |
| tokenized_text | 7,121.0 | +0.2 | 496.8 | -0.7 | 720 |
| all three | 8,273.6 | +1,152.8 | 1,165.2 | +667.7 | 483 |

## Thirty-two clients, three repetitions

Group sizes were comparable across variants (14.4 to 15.9 documents per
commit), so grouping is not what separates these rows.

| variant | throughput vs base | writer busy us/doc |
|---|---|---|
| base | | 288.6 to 294.6 |
| projection | **-80.4%, -79.9%, -81.0%** | 1,027.0 to 1,085.3 |
| text_key | -6.8% | 299.8 to 307.7 |
| tokenized_text | +1.9% | 291.7 |

## What this establishes

**A projection costs writer time, not bytes.** Zero additional bytes per
document at one client, and a throughput cost that grows with concurrency:
-33% at one client, and -80% at thirty-two, reproduced three times within 1.1
percentage points. This is the opposite of amortising, and it is the single
largest per-mechanism cost measured anywhere in this programme.

**A text-key index costs bytes, not time.** +1,153 bytes per document at one
client, near-zero writer time, and -6.8% throughput at thirty-two clients.
Roughly what an extra index entry over a 200-byte field should cost.

**The tokenized text declaration costs nothing measurable.** Zero on bytes and
zero on writer time at both concurrencies. The index is declaration-only in
this build and no contract surface can query it, so this says the declaration
is free to carry, not that a working tokenized index would be.

The three compose additively: the `all` variant matches text_key's byte delta
to the decimal and approximately the sum of the writer-time deltas.

## What this does not establish

**The writer-busy figures are corroboration, not a CPU multiple.**
`crates/riffdb-commit/src/writer_census.rs` states that `busy_us` and `idle_us`
do not tile the writer's wall clock: completion draining after submission lands
in neither counter, and roughly a quarter of `busy` is unnamed by any stage.
The arithmetic shows it -- at the baseline, 294.6 us/doc over 2,000 documents
is 0.59 s of reported busy against 0.38 s elapsed, and the busy-to-elapsed
ratio differs between variants (about 1.56 at the baseline, about 1.10 with a
projection). So the counter moves in the same direction as throughput and by a
similar order, but "N times the writer CPU" is not a claim this measurement
supports. The throughput figures are measured directly and do not depend on it.

**One workload shape.** See below.

**One workload, one host, one document shape.** Publishing into a single
workspace, so every publisher contends on one conflict key. A workload spread
across many partitions could behave differently.

## Where the projection cost actually is

The writer batch-stage census (`RIFFDB_WRITER_BATCH_DIAGNOSTICS=1`) locates it
in one stage. Base against projection, 500 documents at 32 clients, 501
commands each:

| stage | base us/cmd | projection us/cmd | delta |
|---|---|---|---|
| `unit_execute` | 263.3 | 596.8 | +333.4 |
| `exec_batch_begin` | 0.3 | 330.5 | **+330.2** |
| `exec_apply` | 30.9 | 31.5 | +0.6 |
| `exec_evaluate` | 11.4 | 11.8 | +0.4 |

`exec_batch_begin` accounts for 99 percent of the added time. Evaluation and
apply are unchanged, so the command path is not doing more work; it is waiting
before it starts.

`exec_batch_begin` wraps `port.begin_empty_batch()`, which reaches
`begin_attributed_write` in `crates/riffdb-storage-redb/src/store.rs:5061`.
That acquires `shared.mutation_gate` and then `database.begin_write()`, and
redb permits one write transaction at a time. Projection application is not
part of the command's transaction: `apply_projection`
(`crates/riffdb-storage-redb/src/derived.rs:648`) opens its **own**
`begin_attributed_write`, through the same gate.

So the cost is contention, not projection arithmetic. The command writer
serialises behind the projection worker's write transaction on a single-writer
store. Four observations agree with that and none contradicts it: the whole
delta is in gate acquisition; per-command evaluation and apply are flat; the
projection adds no bytes to the command frame, because its rows are written in
a different transaction; and the cost grows with concurrency, which is what
contention does and what per-command work does not.

What is measured is the stage attribution and the call path. The exact
interleaving of the two writers is inferred from them rather than traced, and
a contention trace would settle it.

This points at the shape of any fix -- batching projection applies, sharing the
command's transaction, or applying less often -- rather than at making
projection evaluation faster, which the census says costs almost nothing.

## Reproducing

```
cargo build --release --bin riffdbd
cd benchmarks/perf-surface && cargo build --release
PERF_SURFACE_DOCUMENTS=2000 PERF_SURFACE_CONCURRENCY=32 \
  ./target/release/riffdb-perf-surface
```

Read byte figures at one client and throughput under load: per-group framing
overhead spreads over however many documents a group carries, so a
byte-per-document delta measured under concurrency mixes the mechanism with
the grouping.
