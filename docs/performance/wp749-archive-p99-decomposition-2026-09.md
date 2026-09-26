# WP-749: P99 decomposition from retained requests

**Current disposition: blocked on [WP-810](wp-810-checkpoint-journal-tail.md).**
The maintainer ended subtraction-based investigation on 2026-09-21 and accepted
the [named-mechanism disclosure and blocked handoff](wp749-archive-qualification-2026-09.md#blocked-handoff-and-disclosure-2026-09-21).
The arithmetic below is retained evidence of the instrument's identification
limit. It is not a request for further analysis. After the shared term is
reduced, the defined isolated pair measures archive acknowledgement cost
directly.

**The data do not establish a small, named archive-specific residual. The
unchanged-acknowledgement-latency gate remains unmet.** The observed P99 movement
is **26.582760 ms**. Request-by-request subtraction of the measured pre-sync
wait changes that contrast by **14.895930 ms**, leaving **11.686830 ms**.
These are exact arithmetic results on the retained traces, not an identified
causal split into shared and archive-specific time.

The shared checkpoint/journal finding now has its own open package,
[WP-810](wp-810-checkpoint-journal-tail.md). This analysis used the existing
[host-valid isolated pair](wp749-archive-isolated-pair-2026-09.md); no new run,
host reservation, runtime change or gate amendment was made.

## What the pre-sync interval accounts for in the observed traces

For each measured request, join its command sequence to the actual group and
partition the first receipt wait into before-sync, inside-sync and after-sync
intervals. Subtract only its own before-sync interval when checkpoint drain is
required, then **re-sort every request and calculate P99 again**. Do this in
both arms, including the disabled arm's small pre-sync interval.

| Request distribution | Disabled P99 (ms) | Enabled P99 (ms) | Difference (ms) |
| --- | ---: | ---: | ---: |
| Original acknowledgements | 88.347260 | 114.930020 | 26.582760 |
| After subtracting own checkpoint pre-sync wait | 88.226500 | 99.913330 | 11.686830 |

Thus **26.582760 - 11.686830 = 14.895930 ms** is the change in the contrast
when those measured intervals are removed from the samples. This names an
observed blocking location associated with a substantial part of the movement.
It does not establish that archive caused every removed interval, or predict
the system's latency after a fix. The calculation leaves all other durations,
queueing and the observed schedule fixed; a real change would not.

The retained example makes the location concrete: enabled group 12,256,
sequences 212,986–213,004, has a **141.387 ms positional journal write inside a
142.265 ms receipt wait**, while checkpoint 61 is registered. Sync takes only
**0.799 ms**. Its nineteen command acknowledgements take 160.705–162.021 ms.
The write is uniquely assigned by interval containment with one frame per
flush. The write itself remains an observed fact; its kernel/device cause
remains unmeasured.

## How much is simply changed checkpoint-drain exposure?

The fraction of measured requests whose own group requires drain increased
from **176,726 / 263,888 (66.9701%)** to **164,134 / 240,636 (68.2084%)**.
To isolate that change descriptively, retain every observed latency within
each drain state and change only the weight assigned to each state. These are
request-weighted distributions, not equally weighted group distributions.

| Observed conditional latency distributions | Disabled drain fraction | Enabled drain fraction |
| --- | ---: | ---: |
| Disabled cell | 88.347260 ms | 92.230820 ms |
| Enabled cell | 112.236720 ms | 114.930020 ms |

Each entry is the P99 of the entire weighted request distribution. Integer
sample weights avoid roundoff at the 99% boundary. The diagonal reproduces
the original P99 values exactly. Changing both factors gives two exact paths:

| Order of change | Drain-fraction component (ms) | Within-state latency component (ms) |
| --- | ---: | ---: |
| Drain fraction first, then conditional latencies | 3.883560 | 22.699200 |
| Conditional latencies first, then drain fraction | 2.693300 | 23.889460 |
| Equal average of both orders | **3.288430** | **23.294330** |

Each row sums to **26.582760 ms**. Merely changing the observed own-group
drain fraction therefore explains **2.693–3.884 ms** in this standardization;
it does not explain most of the movement. The larger component is a change
in latency *within* the drain states. It can include changed shared journal
severity, delays behind earlier groups and archive effects. It must not be
renamed "archive-specific residual".

These two analyses are alternatives on the same data. **Do not add the
3.288430 ms exposure component to the 14.895930 ms subtraction result.** They
are not disjoint intervals. No unique causal allocation is obtained by
averaging the orders either.

## Why a causal shared-versus-archive number is not identified

The 26.582760 ms is already an enabled-minus-disabled contrast; the existence
of the shared baseline delay does not create a separately subtractable portion
of that contrast. Its exposure or severity must change to explain a movement.
The standardization measures only the observed own-group exposure component.

The traces retain client elapsed durations, command-sequence joins and server
group/checkpoint intervals. They do not retain aligned request admission and
queue intervals or the counterfactual schedule without an archive worker.
A request can wait behind another group's checkpoint drain even when its own
group requires none. Indeed, disabled requests with no own-group drain reach
189.090 ms, although those groups' own receipt waits never exceed 4.036 ms.
Classifying them as unexposed would discard propagated delays without evidence.

Percentiles also change which request supplies the ranked value. Subtracting
each request's entire own checkpoint receipt wait leaves P99 at 35.971280 ms
off and 67.481340 ms on: a **31.510060 ms** contrast. That larger contrast is
not a negative causal cost of checkpointing; it reflects changed ranks and
remaining durations. Neither group-wait P99 differences nor tail means can
be subtracted from request P99 to manufacture an additive explanation.

Consequently, **11.686830 ms is the remaining fixed-trace contrast after the
pre-sync subtraction, not a proven archive-specific cost or a bound on it**.
The evidence cannot demonstrate that the causal archive residual is small.
It cannot prove that all of the remainder belongs to archive either.

## Closure disposition and checks

WP-810 owns resolution of the shared checkpoint/journal mechanism independently.
WP-749's incremental acknowledgement movement is not fully attributed, so this
analysis does not justify amending its gate around a small named residual.
ADR-0249 supplies no numeric P99 rejection threshold; the unresolved mechanism
and existing latency clause are the reason the gate remains unmet. This is
review evidence, not a qualified performance or package-completion claim. No
additional hypothesis run is opened by the result.

The reproducible bank is
`release/evidence/wp-749/archive-p99-decomposition-20260921/README.md`.
It binds all **504,524** original requests to the prior summary and raw-input
digests, verifies identical executables and valid host receipts, retains both
factor-change orders and records the causal residual explicitly as unknown.
Ten semantic analyzer tests cover quantile rank changes, exact weighted mass,
unequal sample sizes, single-factor changes, interval overlap, and refusal of
changed inputs, host-invalid evidence and invalid subtractions.

## PR note

Package WP-749; tier surface under existing measurement ADRs. Behavior: none;
this handbook page and the linked qualification page record arithmetic and
its identification limits. Checks: retained-input digests, all-request interval
validation, exact endpoint/partition identities, ten analyzer tests and package
documentation acceptance. Compatibility: no runtime, surface or durable-byte
change. Hazards: propagated queueing and the causal archive residual remain
unidentified; no merge, amendment or closure is implied.
