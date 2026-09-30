# WP-818: ordered writer CPU investigation

Status: complete (2026-09-30).
Base: `7eb4b37e1` (WP-814 closure). The 60-second alternating comparison
measures a 5.27% write-only gain; PostgreSQL write parity remains unmet.
Measurement machine: 8 vCPU, AMD EPYC 9B14, the ADR-0239 development standard.

## Pre-change profile

The release daemon and app-baseline runner were rebuilt from the base. The
workload is TicketDesk `--full --load write_only --load-clients 32`, with a
60-second measurement and 10-second warmup, one repetition, and no PostgreSQL.
The daemon is restarted after seeding. This covers the public command path in
that workload; it does not establish performance for projections, tokenized
text, or row-policy evidence.

An initial unprofiled run measured 3,512 operations/s, p95 8.39 ms, p99 12.06 ms,
and maximum 1,026 ms. Its attempted sampler did not attach. A second sampler
attempt caught a setup generation and produced no usable CPU evidence. Neither
attempt is a CPU profile or a candidate comparison.

The successful profile selects the busiest command thread after the nonempty
seed process shuts down. Linux truncates the writer, coordinator, completion,
and preparation thread names to the same prefix, so the prefix alone cannot
identify the writer. The selected thread was sampled for 25 seconds at 997 Hz
with `task-clock`, DWARF stack capture, and a 16 KiB stack sample. It produced
18,260 samples with zero lost samples. The profiled run measured 3,397
operations/s, p95 8.91 ms, and p99 16.78 ms. Profiling is diagnostic evidence,
not an A/B arm or a qualified comparative result.

Selected self sample shares:

| Symbol | Writer samples |
|---|---:|
| SHA-256 compression | 7.56% |
| Overlay node `Arc::make_mut` | 2.96% |
| jemalloc malloc | 2.55% |
| jemalloc extent heap removal | 2.29% |
| redb page lookup | 2.04% |
| jemalloc sized free | 1.85% |
| Overlay point resolution | 1.37% |
| Overlay node insertion | 0.90% |

User-space call stacks from this release build do not unwind sufficiently to
attribute SHA compression to specific callers. Self shares do not establish a
source-level cause. The profile also contains file-read syscall samples; they
are not attributed to a particular table without further instrumentation.

## First hypothesis and probe

Measure checkpoint point reads whose requested key is above the captured
snapshot's last table key, or whose captured table is empty. A bounded probe
in `checkpoint_root.rs` runs under the existing opt-in query diagnostics. It
still performs every original point lookup. A successful last-key observation
belongs only to its immutable snapshot, retains at most 256 key bytes per
table, and emits no keys or values. Oversized keys and failed observations do
not supply absence evidence.

`riffdb-checkpoint-point-read-probe-v1` emits numeric table tag, call count,
miss count, above-last count, total point-get nanoseconds, and above-last get
nanoseconds. The second probe adds exact repeated-miss count and repeated-miss
get nanoseconds. These are separate evidence lines; the existing 15-field writer
checkpoint census is unchanged. Root-local lines must be summed over the
measurement generation separately from reset and seed generations. Diagnostic
runs cannot be compared as production throughput arms.

The first probe (`efe71317`) measured 7.44 seconds of byte-table point-get
work across setup and measurement, of which only 0.23 seconds met the last-key
or empty-table condition. No last-key optimization was implemented. Table 18
(idempotency locators) accounted for 799,449 reads, all absent, and 2.75 seconds
of get work. These totals include seed work and are diagnostic observations,
not production throughput figures or a writer-only cost attribution.

The next probe tracks whether an exact absent table/key was already observed
in the same checkpoint. It uses 256 direct-mapped slots with keys capped at
256 bytes. Full tag/key equality is mandatory, so hash collisions only evict
evidence. This probe still performs the original read, counts its time, and
retains no values. A deterministic shared collision schedule and an exact-key
collision test cover the probe. Its result does not authorize an absence fast
path until the saved work is measured and snapshot semantics are tested.

The repeated-miss probe (`d89a7fea`) measured 341,598 repeated idempotency
locator misses taking 0.70 seconds and 291,357 repeated entity misses taking
0.32 seconds. Including the cheap empty-table misses, repeated get work totals
about 1.04 seconds across setup and measurement. No production negative cache
was implemented: its hashing and locking would also have a cost, and this
observation does not support a gain above the host's variance.

The next diagnostic uses the repository's profiling build with forced frame
pointers to attribute allocation and hashing to callers. Comparative evidence
continues to require the release build.

## Caller-attributed diagnostic and next experiment

A diagnostic binary from `7eb4b37e1` was built with the repository's profiling
profile and `RUSTFLAGS="-C force-frame-pointers=yes"`. It was sampled for 25
seconds at 997 Hz, producing 18,187 samples with none lost. The load completed;
postprocessing inline expansion took several minutes. Disabling inline
expansion makes caller analysis practical. The later inspection happened after
sampling and load completion. Profiling-build throughput is not comparative
evidence.

Selected inclusive writer sample shares (nested shares must not be summed):

| Path | Inclusive samples |
|---|---:|
| Checkpoint point reads | 11.82% |
| Overlay insertion | 5.34% |
| Changelog successor preparation | 5.56% |
| Small-object allocator cache refill | 3.73% |
| Allocator slab-heap removal | 2.12% |
| Overlay checkpoint rebase | 2.74% |

The next scratch experiment changes the existing pinned allocator's bounded
small-object cache capacity, with minimum and maximum both set to 1,000
slots. Both arms use identical release binaries from `7eb4b37e1`; the scratch
candidate selects these already-supported allocator settings before process
startup. The normal maximum is 200. The run confirms option acceptance before
measurement. No production default, new dependency, or public configuration
interface has been adopted. This tests cache capacity as one mechanism; it
does not establish that allocation count, hashing, or overlay construction
has changed. Memory and CPU remain required comparative disclosures.

The allocator experiment finished all twelve release runs, in
base/candidate/candidate/base order for each load, at 32 clients, 60 seconds
measured and 10 seconds warmup. The identical binaries were built from
`7eb4b37e1`. Median operations/s:

| Load | Base arms | Candidate arms | Candidate/base median |
|---|---|---|---:|
| Write-only | 3,496; 3,463 | 3,525; 3,556 | 1.0175 |
| Interactive | 16,227; 16,074 | 15,942; 15,953 | 0.9874 |
| Read-only | 25,004; 24,828 | 25,009; 25,105 | 1.0057 |

The small write delta and worse interactive result do not justify adopting
these settings. Read-only candidate heap-envelope observations were 351 and
366 MB, versus 28 and 54 MB at base. No same-run PostgreSQL qualification is
claimed for this rejected experiment.

Sampling the busiest command thread's CPU ticks during the load process and
normalizing by process-scope committed commands (including warmup) measured
approximately 206.8/207.7 microseconds per command for write-only base and
205.3/202.6 for candidate. Interactive measured 272.3/273.7 at base and
269.3/268.9 for candidate. The sampling interval is 0.5 seconds and tick
resolution 10 ms, so these are approximate endpoint observations. They do not
support treating 266 microseconds as a workload-independent writer cost or
claiming that writer CPU alone limits throughput.

A separate numeric backend-read probe counts calls and requested bytes below
redb's page cache. It is selected once when the backend opens under the existing
diagnostic switch, passes through every original read and error, and emits
`riffdb-backend-read-probe-v1` at backend drop. Counts include all reader threads through backend drop, including shutdown,
and exclude reads performed internally by checkpoint write comparison. Seed
and load generations are separate. Disabled and concurrent probe tests pass.
An initial report parser missed the runner's stderr prefix; inspecting the
complete logs recovered both censuses. The unnecessary close-entry hook is
removed; the original backend-drop hook emitted correctly.

## Primary page-cache capacity experiment

The isolated scratch candidate changes only the primary redb page-cache
capacity from 4 to 12 MiB. The derived database remains at 4 MiB. This adds no
configuration knob and changes no format, transaction, validation, publication,
or acknowledgement rule. The accepted lifecycle ceilings remain requirements.

In two diagnostic c32 write-only runs (60 seconds, 10 seconds warmup), the
measurement generation at 4 MiB made 3,735,805 backend read requests for
15,493,042,505 bytes across 249,885 process-scope committed commands. At 12 MiB
it made 1,791,553 requests for 7,387,173,193 bytes across 253,689 commands.
That is 14.95 versus 7.06 requests per command, a 52.7% reduction. These include
all reader threads and warmup, so they establish reduced backend reads for
this workload, not a writer-only CPU attribution. Diagnostic throughput was
3,476 versus 3,527 operations/s; single runs do not establish a comparative
throughput result. The next batch alternates release base/candidate/candidate/
base for write-only, interactive, and read-only, with diagnostics disabled.

All twelve production arms completed. Each load ran base/candidate/candidate/
base at 32 clients, 60 seconds measured and 10 seconds warmup, with diagnostics
and allocator overrides disabled. Base binaries came from `7eb4b37e1`; the
scratch candidate came from `c68062e0` with only the primary constructor's
budget changed to 12 MiB. The corresponding committed candidate is
`fe6070308`, used for the clean-checkout PostgreSQL qualification below.

| Load | Base ops/s median | Candidate ops/s median | Throughput ratio | p95 ratio | p99 ratio |
|---|---:|---:|---:|---:|---:|
| Write-only | 3,473 | 3,656 | 1.0527 | 0.9265 | 0.8364 |
| Interactive | 16,059 | 16,164.5 | 1.0066 | 0.9474 | 0.9474 |
| Read-only | 24,784 | 24,902 | 1.0048 | 1.0000 | 1.0000 |

Write-only arms were 3,469/3,477 at base and 3,623/3,689 at candidate. The
5.27% median gain exceeds both pairs' within-arm spread. Approximate writer
CPU medians were 208.36 and 198.49 microseconds per command, a 4.74% reduction.
Interactive writer CPU medians were 272.16 and 260.38 microseconds. Interactive
and read-only throughput differences are within variance; no gain is claimed
for them. The candidate p95/p99 observations do not regress either workload.

Median peak sampled RSS was 1,160,833,024 versus 1,187,020,800 bytes for
write-only; 998,563,840 versus 1,019,129,856 for interactive; and 76,683,264
versus 86,130,688 for read-only. These are loaded-process observations, not
lifecycle heap-envelope measurements. Write-only maxima remained approximately
0.7–1.0 seconds in both arms; this change does not resolve extent allocation
owned by WP-815.

## Same-run PostgreSQL qualification

The clean `fe6070308` candidate ran against PostgreSQL 18.4's `safe-app`
comparator at 32 clients, three 90-second repetitions with 15-second warmup,
using the qualified runner and `--require-stable`. Interactive adds
`--load-journeys`. Both commands exited successfully; the reported metrics
were stable. Ratios divide the medians in `load_rep_summaries`.

| Load | RiffDB ops/s | PostgreSQL ops/s | Throughput ratio | p95 ratio | p99 ratio |
|---|---:|---:|---:|---:|---:|
| Write-only | 3,611 | 4,703 | 0.7678 | 0.7750 | 1.1429 |
| Interactive | 16,139 | 15,399 | 1.0481 | 0.8710 | 0.8182 |

The interactive PERF-008 throughput and p95 gate passes. Write-only remains
below PostgreSQL. Its p99 is 16.78 ms versus PostgreSQL's 14.68 ms; the lower
p99 in the shorter alternating experiment is not evidence of a universal
write-tail improvement. The matched 90-second alternating write-only check
below also improves throughput and p99 against base. These PostgreSQL ratios
are a same-run candidate comparison;
comparing them to earlier sessions' ratios does not measure the cache gain.

## Matched 90-second write-only comparison

A further production base/candidate/candidate/base sequence used 32 clients,
90-second measurements, and 15-second warmup. Base remains `7eb4b37e1`;
candidate is `5c3dfd9f3`, with the same 12 MiB cache and an inactive benchmark
inventory error probe. Diagnostics and allocator overrides were disabled.
The candidate daemon digest is identical to the clean `fe6070308`
PostgreSQL-run daemon. All four arms exited successfully.

| Metric | Base median | Candidate median | Candidate/base |
|---|---:|---:|---:|
| Throughput (ops/s) | 3,459.5 | 3,609.5 | 1.0434 |
| p95 (ms) | 8.913 | 8.258 | 0.9265 |
| p99 (ms) | 15.991 | 15.204 | 0.9508 |
| Approximate writer CPU (microseconds/command) | 210.29 | 198.69 | 0.9448 |

Base throughput was 3,422/3,497 operations/s; candidate was 3,601/3,618.
The gain survives both bracketing base arms. Candidate p99 was 14.68/15.73 ms
versus 15.73/16.25 ms at base. The PostgreSQL run's higher candidate p99 is
not reproduced as a base-relative regression here. Maximum latencies were
719/746 ms at base and 720/1,015 ms at candidate; long stalls remain and no
maximum-latency improvement is claimed. The binary identities and individual
numeric observations are recorded in
`release/evidence/wp-818/closure-20260930.json`.

## Lifecycle qualification gap

An additional qualified lifecycle run used the existing 65,536-row fixture
(`--production --organizations 4 --tickets-per-project 86
--labels-per-ticket 8`). It failed closed when the stopped clean twin's
read-only authority snapshot could not open redb. An opt-in numeric error
probe identified `RepairAborted`. The unchanged `7eb4b37e1` daemon reproduces
that failure in the same harness at a stopped snapshot, so it is not specific
to the cache candidate. No repair, writable oracle, or skipped authority check
was substituted. No new successful lifecycle or production-size resource
qualification is claimed.

The current V2 substrate retains ADR-0260's bounded dirty readiness path and
refuses a bounded clean certificate. Recorded startup diagnostics reported
`complete_validation`, no population walk, one cold source, no activations or
population passes, and 149 recovery-owned bytes. This is distinct from the
loaded process RSS above. Existing semantic and architecture tests preserve
the lifecycle rules, fixed cache budgets, and 384 KiB production stacks. The
failed additional qualification remains a handoff hazard, not proof of the
PERF-019 referee result.

## Review note

Package: WP-818. Tier: guarantee (the primary constructor is a guarantee-tier
path). The cache budget is an implementation choice under D-003 within the
existing accepted records; their decisions and ceilings are unchanged.
Behavior: a fixed 12 MiB primary cache, with the derived cache still 4 MiB,
plus bounded opt-in numeric instrumentation. Backend-read measurements support
reduced page reads; they do not attribute all improvement to writer-only CPU.

Checks: the full `ci-all` battery passed with `RUST_TEST_THREADS=4`, including
process crash, recovery, replication, idempotency, and provenance coverage.
The final package-scoped acceptance run passed all 13 steps. Its storage
suite passed 1,092 tests, with one existing ignored shutdown-scale diagnostic.
The initial full CI attempt timed out waiting for replication attachment; the unchanged isolated
test and the complete run with four test threads passed. No assertion or
deadline was weakened by this package. The standalone benchmark runner's
existing unused-import and missing-documentation warnings were also present
at base; no new warning is introduced by the cache change.

Compatibility: no protocol, public interface, durable encoding, stack-size,
validation, transaction-order, or acknowledgement change. Updated handbook
pages are this report, `docs/performance/perf-013-014.md`, and the
`docs/SUMMARY.md` index. Hazards: write throughput remains below PostgreSQL;
maximum write stalls remain; the additional lifecycle oracle fails at base
and candidate; projection, tokenized-text, and row-policy performance are not
qualified here. WP-815, WP-816, and the alternate-machine gate remain outside
this package. Other work merged into main after the measured candidate; the
performance figures apply to the named commits, not those unrelated changes.
