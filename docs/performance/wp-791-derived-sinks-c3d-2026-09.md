# WP-791 derived sinks: C3D write-throughput measurement

The bench-host measurement OBL-0240-3 defers to. Taken on the C3D bench
host (AMD EPYC 9B14, 8 vCPU, ADR-0239 dev standard) with
`benchmarks/perf-surface`, 2,000 published documents per variant, 32
clients, revision `c29722f1e`, `riffdbd` digest `7e2130e23af1e658`,
2026-09-20.

The diagnosis this package implements against is
[per-mechanism write cost](perf-surface-mechanism-costs-2026-09.md): on
this host, before WP-791, declaring a projection cost **-80.4%, -79.9%,
-81.0%** of write throughput at 32 clients, and -33% at one client.

## Result

Twelve repetitions, 32 clients. Each repetition is one process that runs
every variant in a fixed order against its own daemon and its own fresh
database, so the deltas are within-repetition.

| mechanism | mean | sd | min | max |
|---|---:|---:|---:|---:|
| projection | **-3.27%** | 2.12 | -7.20% | -0.50% |
| text_key | -6.62% | 1.97 | -9.50% | -4.00% |
| tokenized_text | -0.64% | 1.42 | -3.20% | +1.30% |
| vector | -19.95% | 1.72 | -23.00% | -17.30% |
| all four | -28.07% | 1.32 | -30.00% | -26.20% |

Absolute throughput over the same twelve repetitions:

| variant | mean docs/s | range | range as % of mean |
|---|---:|---|---:|
| base | 5,544 | 5,442-5,636 | 3.5% |
| projection | 5,363 | 5,048-5,451 | 7.5% |
| tokenized_text | 5,507 | 5,436-5,598 | 2.9% |

At one client, three repetitions: **-2.4%, +1.5%, +0.5%** against a base
of 1,045, 1,027 and 1,012 documents per second. The -33% one-client cost
is gone outright.

Every projection repetition asserts catch-up. `drain_projection` polls
`GetProjectionStatus` until the published frontier reaches the
authoritative head and **fails** the run if it does not, if the status
reports a failure, or if the projection is absent; the drain (874-1,014
ms at 32 clients) is reported separately and is outside the timed publish
phase. This is what the first local run lacked and was withdrawn for.

## Whether OBL-0240-3 is discharged

The obligation reads: *a contract declaring a projection sustains write
throughput within the bench host's run-to-run spread of one that declares
none.* Which estimator "the host's run-to-run spread" names decides the
answer, and the two available readings disagree:

- **Against base's own run-to-run spread.** Base varies 3.5% across the
  twelve repetitions. The projection mean is 3.27% below base. Inside,
  narrowly. **Passes.**
- **Against a mechanism known to cost nothing.** `tokenized_text` is
  declaration-only -- no contract surface can query it, and it moves zero
  bytes and near-zero writer time -- so its delta is this harness's
  zero point. It sits at -0.64% (sd 1.42) and projection at -3.27% (sd
  2.12). The difference is 2.62 points, standard error 0.74, t = -3.6 on
  about 22 degrees of freedom. Projection is distinguishable from free.
  **Fails.**

Both are true. Declaring a projection no longer costs 80% of write
throughput; it costs about 2.6 points against the harness's zero point,
reproducibly. That is 96% of the original cost removed and a small
residual that is real rather than noise.

Recording it this way rather than picking the favourable reading, because
the difference between the two is a definition the record does not
currently supply, and the reading that passes is the one that ignores the
control.

Two further signs the residual is a mechanism rather than measurement
noise: the projection cell's own throughput spread is 7.5% against base's
3.5% and the free control's 2.9%, which is what an apply that
intermittently contends looks like; and `writer_busy_us_per_doc` runs
above base in most repetitions.

## A delta's noise floor is not the host's run-to-run spread

Three repetitions were taken first and could not resolve this. They put
projection at -8.8%, -4.6%, -3.0% -- and `tokenized_text`, which does no
work, at +2.0%, +1.3%, -5.0%. A control that swings seven points across
the same three repetitions is a measurement that cannot see a three-point
effect, whatever base-versus-base stability suggests.

The lesson generalises past this package. A delta inherits variance from
both of its cells and from the position each occupies in the sequence, so
the spread of a repeated baseline understates it. The free-mechanism
control measures the whole thing at once, and here it also exposes a
small systematic bias: its mean is -0.64%, not zero, because variants
later in the sequence run slightly slower than the base that opens it.
Any future per-mechanism claim on this harness at a few percent should
carry such a control, and no claim below about 3 points should be made
from three repetitions.

## What else these twelve repetitions establish

**A vector field costs about 20% of write throughput at 32 clients**
(-19.95%, sd 1.72), which is the largest per-mechanism cost now measured
and was not in the banked table -- that table predates the mechanism.
Unexplained and unowned by any package; it needs its own investigation
rather than a footnote here.

**`text_key` reproduces its banked figure.** -6.62% here against -6.8%
banked, on a host and a toolchain that have both moved since. That the
one unchanged mechanism lands within two tenths of a point is the best
evidence available that this harness is still measuring the same thing.

**The mechanisms remain close to additive.** projection + text_key +
vector is -29.8 points against -28.1 measured for all four together.

## What this does not establish

**One workload shape.** Every publisher contends on a single workspace,
so one conflict key. A workload spread across partitions could behave
differently, and the residual is the kind of cost that a different
contention profile could change in either direction.

**Not a release qualification.** One host, one profile. ADR-0239 makes
C3D the dev standard, not a release gate, and
[benchmark host selection](benchmark-host-selection.md) still requires
two profiles for qualification.

**The writer-busy column is corroboration only.** `busy_us` and
`idle_us` do not tile the writer's wall clock
(`crates/riffdb-commit/src/writer_census.rs`), so it is read here as
moving in the same direction as throughput, not as a CPU multiple.
