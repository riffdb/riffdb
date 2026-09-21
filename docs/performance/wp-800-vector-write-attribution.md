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

So the cost is, to a first approximation, the cost of carrying a bigger
command through staging. That is a different claim from "computing an
embedding is expensive", and the evidence separates them: `exec_evaluate`,
which is where deterministic runtime evaluation happens, moves 1.2 us
against staging's 12.4.

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

**Nothing here says the cost is avoidable.** A vector of four f32 with its
model metadata is real payload. Whether 1,940 bytes per document is the
right size for it, and whether staging must copy it the way it does, are
the questions the attribution makes askable.
