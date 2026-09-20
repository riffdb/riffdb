# WP-749 capture investigation: closed on measured headroom

Status: **investigation closed by maintainer direction, 2026-09-19**. WP-749
remains open. Minimal-evidence, allocation/encoding, packing and checked-hash
optimization are closed; no further capture experiment is scheduled. WP-748
promotion and lifecycle consolidation remain paused. P99 remains a hard gate.

## Closure decision

Capture costs **3.75% of independently measured writer wall on C3D with
SHA-NI**. All its hashing accounts for **1.41%**. These are gross work ceilings,
not demonstrated recoverable acknowledgement-latency savings. Capture cleared
the approximately 3% investigation screen; that screen was necessary to
investigate, not sufficient to select a redesign.

The one physical-sharing prototype with a timing result saved **2.22% mean
group-commit time** and **25.78% frame bytes**, but worsened mean per-trial
**P99 by 7.89%**. It fails the tail gate. Checked-hash reuse passed selected
correctness tests but produced **no valid candidate timing or P99 verdict**.
The combination of the small remaining ceiling, failed tail result and absent
selection evidence closes this line under the agreed rules. It does not fund a
new durable format or its retained-value lifetime obligation.

Byte headroom is real; sufficient latency headroom has not been demonstrated.
No prototype is selected, no capture implementation is merged, and no restore
granularity, durable identity or performance gate changes through this closure.

## Reconstruction finding retained

`maintenance/archive_prefix.rs::construct` validates the original receipt and
prefix preconditions against a pinned predecessor.
`maintenance/archive_prefix_graph.rs::derive` folds only commands through the
requested application sequence, reconstructs locators and allocators, checks the
exact mutation inventory and seals a private partial segment. The necessary
information is final net mutations with command ownership plus ordered discarded
intermediates, including all net-zero touches and their first preconditions.
Repeated keys need correct intervening expected-prior hashes. Shared index
epochs are repeated keys even when entity keys differ; their intermediate
values cannot simply disappear. Atomic transaction capture remains necessary.

This proves information sufficiency, not a deployable encoding. Retained readers
in `command_prefix/rows.rs::decode_segment` and `command_prefix/catalog.rs` also
validate complete images without the original receipt. Any future representation
would need an authenticated, bounded value source surviving receipt pruning.
That obligation remains intact; closure does not authorize references to pruned
receipts or mutable current state.

The 27,427-command census modeled 1,744 to 270 prefix bytes per command (84.5%
less), or 26.9% fewer frame bytes. Capsule entity transitions contain versions,
semantic hashes and chain facts, not complete entity values. Full-value
duplication lies between prefix evidence and independent receipt mutations.
The physical-sharing prototype confirmed the byte opportunity, with the latency
and tail outcome above. Allocation plus encoding cost only 31.207 us/group on
N1 and 12.752 on C3D; that line is closed too.

## Host asymmetry is a result in its own right

Same daemon and runner bytes, frozen inputs and Rust 1.98.1; results are kept
separate. These are capture-probe-on aggregates for c32 write-only diagnostics.
N1 is now retired from testing by maintainer direction; its rows are historical.

| Measure | N1: Intel Xeon, no SHA-NI | C3D: AMD EPYC 9B14, SHA-NI |
|---|---:|---:|
| Group commit, us | 8,203.645 | 2,884.361 |
| Independent writer wall/group, us | 12,326.065 | 4,498.561 |
| Capture/group, us | 696.465 | 168.491 |
| Capture / writer wall | 5.65% | 3.75% |
| Inventory, us/group | 86.763 | 47.273 |
| Allocation/copy, us/group | 19.018 | 8.230 |
| Hashing, us/group | 491.040 | 63.294 |
| Validation/attachment, us/group | 29.077 | 14.853 |
| Logical epochs, us/group | 58.377 | 30.319 |
| Prefix encoding, us/group | 12.189 | 4.522 |
| Unattributed writer wall, us/group | 77.220 (0.626%) | 35.283 (0.784%) |
| Busy + idle undercount of wall | 15.30% | 20.54% |

The **7.8-times hashing difference** changes the available optimization budget.
N1 overstates hash-bound cost for SHA-accelerated deployment hardware. Normal
`sha2 0.11.0` / `cpufeatures 0.3.0` dispatch selects hardware hashing on C3D;
the cross-host ratio does not isolate SHA-NI from every other hardware difference.
See [host-selection guidance](benchmark-host-selection.md) before drawing a
deployment conclusion from any measurement host.

Capture stages are exclusive within the probe hierarchy and already included
in parent writer stages. Some capture precedes `commit_started_at`; dividing it
by group latency (8.49% N1, 5.84% C3D) does not partition that timer. Journal
encoding, writes and sync overlap writer work and contain other bytes. No
byte-proportional I/O saving is attributed. Independent wall reconciliation and
the still-open producer defects are documented in the
[writer warning](wp749-writer-census-defects.md).

## Measurement limits and prototype disposition

Each host ran off/on/on/off capture-probe cells: 20-second smoke windows,
one-second warmup, fresh databases, standard durability, no PostgreSQL or archive
consumer. These are bounded diagnostics, not release qualification. All four
timed cells on each host reconciled level-0/1 stages with zero signed accounting
error. Invalid nested level-2 attribution was excluded.

Group means weight physical groups; throughput and P95/P99 summaries average
per-process estimates, not pooled percentile histograms. C3D observer deltas
were -1.10% group mean, -0.78% throughput and 0.00% mean P99; N1 deltas were
+0.15%, -0.39% and +2.70%. C3D probe-on P99 ranged from 75.50 to 92.27 ms,
against 83.89 ms in both controls. Two repetitions establish no candidate tail
pass. One C3D preflight refusal preceded the attribution cells; it was retained,
only unrun cells resumed, and no timed cell was retried.

The separate checked-hash prototype retained a private digest beside immutable
owned values and preserved fresh validation for external predecessors and decoded
segments. It passed 332 selected tests and scoped acceptance. Its first comparison
control was refused at postflight because one process exited; the cause is
unknown. The runner stopped before any candidate timing. Source, tests and the
refusal are banked as unselected research. The comparison is **closed without a
verdict**, not queued for another attempt.

The physical-sharing timing predates this attribution: both compared arms ran
on N1 with Rust 1.97.0. It is historical rejection evidence, not a Rust 1.98.1 baseline.
The [curated numerical summary](evidence/wp749-capture-summary-20260919.json)
retains all eight attribution cells and signed errors. Raw reports, source
patches, binaries and host observations remain in the private evidence bank;
they are not public repository artifacts.

## WP-749's next decision: locate the gap or re-derive the gate

The historical N1 absolute comparison already put then-current main at **1,291
ops/s versus 1,220** for the pre-capture parent. Both used Rust 1.97.0; the
comparison also includes intervening runtime, allocator and release-profile
changes. It cannot isolate capture's present cost or qualify today's main. The
original 27% regression is no longer a defensible estimate of recoverable
headroom. The merged performance programme and Rust 1.98.1 require fresh
controls before any future campaign.

The next review should answer these questions in order:

1. Which exact current gate is still unmet: WP-749's archive-presence effect on
   acknowledgement latency/write admission, the broader application gate, or
   both? Name its source revision, host, metric, tail condition and denominator.
2. Does valid current-baseline evidence locate a remaining gap outside capture?
   Keep absolute before/after write-path evidence alongside the archive-enabled
   comparison: an unconditional cost cancels out of a relative enabled/disabled
   gate. Use independent wall accounting and report the residual.
3. If the gate's baseline or interpretation is obsolete, bring an explicit
   derivation for maintainer review. Keep REP-007, exact-stop guarantees and the
   accepted gate unchanged until an amendment is accepted. Throughput alone
   cannot discharge acknowledgement or P99 requirements.

This closure starts that decision, not another representation or benchmark
campaign. Promotion correctness remains WP-748 work and cannot discharge
WP-749's performance gate. WP-749 stays open; WP-748 stays paused.
