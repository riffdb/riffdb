# WP-800: where a vector field's write cost goes

Declaring a vector field costs **19.95% of write throughput at 32 clients**
on the C3D bench host, reproduced over twelve repetitions with a standard
deviation of 1.72
([C3D measurement](wp-791-derived-sinks-c3d-2026-09.md)). This page
attributes that cost to stages, from instrumented counters rather than
from reading the code.

## The throughput figure is not measurable on this workstation

Six runs of the same shape, 2,000 documents at 32 clients, three with the
writer census on and three with it off:

| census | vector vs base |
|---|---|
| off | -21.8%, -2.1%, -25.2% |
| on | -1.1%, -16.0%, -3.0% |

A 24-point spread on the effect under test. The census is not the
confound -- both conditions swing the same way -- this host simply cannot
resolve it, which is what makes C3D the host the obligation names.

Two things stayed steady across all six runs regardless of the census:
`frame_bytes_per_doc` at about +1,940, and `segment_bytes_per_doc` at
about +982. Bytes are the reliable signal here; throughput is not.

## The stage census resolves it where throughput does not

`RIFFDB_WRITER_BATCH_DIAGNOSTICS=1`, three repetitions, base against
vector, nanoseconds per command. One published document is one command in
this harness, so these are also per document.

| stage | mean | min | max |
|---|---:|---:|---:|
| `unit_execute` (level 0) | +20,923 | +15,759 | +26,056 |
| `drive_total` (parent) | +18,888 | +16,033 | +21,816 |
| **`exec_stage_serial`** | **+12,395** | +11,650 | +13,618 |
| `exec_seal` | +3,307 | +3,215 | +3,370 |
| `exec_evaluate` | +1,195 | +1,114 | +1,339 |
| `serial_evaluate` | +718 | +506 | +968 |
| `exec_outer_residual` | +2,041 | -270 | +4,223 |
| `exec_apply` | +2,032 | -293 | +3,688 |
| `loop_work_recv` | +1,185 | -6,913 | +9,788 |

The top four hold their sign and their rough size across every
repetition; `exec_stage_serial` varies by about 8% of its mean and
`exec_seal` by about 2%. The last three flip sign between repetitions and
carry no attribution.

**Per-command stage charges are stable on a host where throughput is
not.** They measure work done per command rather than wall clock under
contention, which is why the same three runs that put throughput between
-1.1% and -17.2% put staging within 8% of itself.

## What it says

`unit_execute` is by definition the window the `busy` counter reports, and
its +20.9 us per command agrees with the +19.7, +23.2 and +34.8 us per
document that the independent `writer_busy_us_per_doc` figures gave. Two
instruments measured from different places agree, which is the check that
says the parsing is right.

Inside it, **serial staging is about 60% of the added writer time**, with
sealing a distant second. Staging is `stage_first_evaluated_command_on_empty`
and `append_evaluated_command` -- where the evaluated command body is put
onto the open batch. A vector field makes that body about 1,940 bytes
larger, measured, and steady across every run on both hosts.

The obvious reading is that staging costs what it costs because the body
is bigger. That reading is wrong, and the next section is how.

## Staging does not scale with the payload

Widening the embedding and re-measuring, three repetitions per width, on
this workstation:

| width | added bytes | added staging | run-to-run spread | added `unit_execute` |
|---|---:|---:|---:|---:|
| 4 | +1,939 | +14,064 ns | 2,036 | +23,873 ns |
| 16 | +2,082 | +16,141 ns | 2,010 | +30,424 ns |
| 64 | +2,657 | +16,258 ns | 1,962 | +32,226 ns |

Sixteen times the vector, 37% more bytes, and **16% more staging** -- a
step comparable to the spread between repetitions of the same width. From
16 to 64 the added staging moves 117 ns while the body grows 28%.

So the added staging is dominated by a **fixed cost of the entity having a
vector field at all**, not by carrying its payload. Whatever it is, it is
paid per command and is nearly indifferent to how much vector there is.

`unit_execute` tells the other half: it rises 35% from width 4 to 64, so a
genuinely width-dependent cost does exist -- just not in staging. The
first reading had the largest stage and the obvious mechanism and put them
together, which is what made it wrong.

This is what the attribution was for. A fixed per-command cost and a
carriage cost want different fixes, and nothing in the first table
distinguishes them.

## Inside staging

`exec_stage_serial` was one number, so the append path it charges was given
level-2 counters, and the two that mattered were split again. They are all
appended past every residual range in the census, because they are children
of stages that already have totals: put inside a residual's run they would
be double counted, and the residual they fell into would read as attributed
when it is not.

Three repetitions, nanoseconds per command, vector against base:

| stage | added | spread |
|---|---:|---|
| `exec_stage_serial` (level 1) | +14,336 | 13,352-14,990 |
| &nbsp;&nbsp;`append_build_stage` | +8,224 | 7,585-8,636 |
| &nbsp;&nbsp;&nbsp;&nbsp;**`stage_attach`** | **+8,710** | 8,162-9,139 |
| &nbsp;&nbsp;&nbsp;&nbsp;`stage_build_records` | +442 | 345-502 |
| &nbsp;&nbsp;`append_epoch` | +3,380 | 3,303-3,433 |
| &nbsp;&nbsp;&nbsp;&nbsp;`epoch_reserve_assign` | +2,373 | 2,322-2,458 |
| &nbsp;&nbsp;&nbsp;&nbsp;`epoch_derive_indexes` | +955 | 918-1,023 |
| &nbsp;&nbsp;&nbsp;&nbsp;`epoch_read_affected` | +47 | 36-57 |
| &nbsp;&nbsp;`append_current` | +785 | 661-940 |
| &nbsp;&nbsp;`append_authorize` | +457 | 406-490 |
| &nbsp;&nbsp;`append_begin` | +325 | 197-412 |

The named children account for about 95 percent of the stage above them.
`stage_attach` exceeds `append_build_stage` because it charges every call to
`stage_checked_candidate`, including the once-per-group first-on-empty path
that `append_build_stage` does not cover.

**Building the record set is not the cost.** `stage_build_records` is where
the atomic command record set is built -- where a vector's bytes are
encoded -- and it adds 442 ns against attach's 8,710. Reading the affected
epoch adds 47. Whatever declaring a vector field costs the writer, it is
not encoding the vector, which is what the width-independence already
implied and this measures directly.

The cost is in two places:

1. **`candidate.stage(records)`, attaching the built records to the open
   batch: +8.7 us**, 60 percent of the added staging.
2. **Reserving capacity and assigning the sequence: +2.4 us**, 17 percent.

Both are batch accounting rather than payload work, and both are paid per
command by an entity that declares a vector field, whatever it puts in it
and however wide that is.

## A second admitted profile

E2 (`instance-20260815-e2`, AMD EPYC 7B12, 8 vCPU) was admitted against
ADR-0245 by reading its capabilities from the host rather than its name:
SHA-2, AES, carry-less multiply and SSE4.2 all present. Six repetitions,
2,000 documents at 32 clients, revision `17bebc236`:

| rep | base docs/s | vector docs/s | delta |
|---|---:|---:|---:|
| 1 | 2,216 | 1,937 | -12.6% |
| 2 | 2,228 | 1,971 | -11.5% |
| 3 | 2,110 | 1,879 | -11.0% |
| 4 | 2,270 | 1,993 | -12.2% |
| 5 | 2,278 | 1,797 | -21.1% |
| 6 | 2,327 | 1,836 | -21.1% |

**The run is bimodal and the mean hides it.** The first four cluster
between 11.0 and 12.6 percent; the last two sit at 21.1. Reporting -14.9
percent as the figure would describe no repetition that happened. Whatever
moves between the fourth and fifth repetition is unexplained and is the
first thing a further E2 run should hold still.

The two 21.1 percent cells are not a duplicated row: they come from
different pairs, 1,797 against 2,278 and 1,836 against 2,327, which agree
to one decimal by coincidence.

Taken with C3D's -19.95 percent over twelve repetitions, two admitted
profiles agree that declaring a vector field costs write throughput
materially, in the same direction, at the same order of magnitude.

## The leading attribution ports; the ranking below it does not

The same probe on E2, three repetitions, nanoseconds per command:

| stage | E2 | this workstation |
|---|---:|---:|
| `unit_execute` | +91,124 | +20,923 |
| **`exec_stage_serial`** | **+49,552 (54%)** | **+12,395 (59%)** |
| `exec_apply` | +32,311 (35%) | +2,032 (sign-flipping) |
| `exec_seal` | +9,730 | +3,307 |
| `exec_evaluate` | +3,000 | +1,195 |

Serial staging is the largest added stage on both hosts and takes a
similar share of the total, which is the part of the attribution that
ports. `exec_apply` does not: it is a third of the added time on E2 and
indistinguishable from noise on the workstation.

E2 charges about four times the workstation per command throughout, which
is also why it resolves a throughput effect the workstation cannot. A
slower host makes the same work a larger share of a longer command.

**So the headline holds on two hosts and the second place does not.** Any
attribution below the leading stage needs to name the host it was taken
on.

## What this does not establish

**The attribution is this workstation's; the figure is C3D's.** The two
have not yet been taken on the same host. Stage attribution is per-command
work and ought to port better than throughput does, but "ought to" is not
a measurement, and the same probe needs a C3D run before this is a closed
question.

**A share of the added time is not named.** `unit_execute` is +20.9 and
the stable named children sum to about +17.6, leaving roughly 3 us in
residuals that flip sign. That remainder is honest noise at this sample
size, not a hidden stage, but it is not nothing either.

**Why attaching and reserving cost what they do is still unidentified.**
The split says where the fixed cost is paid and rules out the payload; it
does not say what attach and reserve are doing differently. That both are
batch accounting, and that neither moves with the vector's width, points at
the number of records or entries a vector field adds rather than their
size -- but that is a hypothesis to instrument, not a finding. The first
reading of this data was already wrong in exactly the way that pairing the
biggest number with the likeliest story is wrong.

**The widths were measured on one host and one rep count.** Three
repetitions resolve a 16% step against a 14% spread only just. The
direction is clear and the magnitude is not.
