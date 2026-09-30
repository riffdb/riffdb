# WP-818: ordered writer CPU investigation

Status: open. Base: `7eb4b37e1` (WP-814 closure). No throughput improvement
is claimed until alternating comparisons and same-run PostgreSQL checks finish.
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

A separate numeric backend-read probe counts calls and requested bytes below
redb's page cache. It is selected once when the backend opens under the existing
diagnostic switch, passes through every original read and error, and emits
`riffdb-backend-read-probe-v1` at backend drop. Counts include all reader threads
and exclude reads performed internally by checkpoint write comparison. Seed
and load process generations must be kept separate. Disabled and concurrent
probe tests pass; the probe supplies no read answer or retained page cache.

## Review note

Package: WP-818. Tier: internal. Current behavior change: opt-in numeric-only
instrumentation; production point-read answers and durable bytes are unchanged.
Checks and experiment results will be recorded as work proceeds. Compatibility:
no protocol, public interface, durable encoding, stack-size, or acknowledgement
change. Handbook behavior is unchanged; this document describes internal
measurement methodology. Hazards: write throughput remains below the reported
same-run PostgreSQL result; no optimization is yet established. WP-815,
WP-816, and the deferred alternate-machine gate are outside this package.
