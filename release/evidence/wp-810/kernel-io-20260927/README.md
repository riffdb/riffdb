# WP-810 native checkpoint/journal I/O diagnostic

This diagnostic follows the complete backend trace in
[`../materialization-capacity-20260926/`](../materialization-capacity-20260926/).
It asks which native operation holds journal sync while checkpoint sync is
active: file writeback, ext4 transaction-commit completion, or device flush.
It uses the same fully qualified application executables from source
`31aeab76`, Rust 1.98.1, and the same full c32 write-only workload: 15-second
warmup, 90-second measurement, archive disabled. Later main changes are not
silently substituted into these source-bound observations.

## Method and limits

`kernel_trace.bt` uses bpftrace 0.25 tracepoints and typed kernel function
entry/return probes. Syscall records retain process/thread identity, descriptor,
ext4 device/inode, timestamps and result. Nested intervals observe:

1. `file_write_and_wait_range`: write dirty file pages and wait for completion.
2. `jbd2_log_wait_commit`: wait for the identified ext4 journal transaction.
3. `blkdev_issue_flush`: issue and wait for a device cache flush.

JBD2 transaction phases and block issue/completion events are retained too.
Block events lack a request pointer and can include partial completion; the
analyzer keeps their populations separate instead of inventing latency pairs.
All native intervals use the kernel clock. Application receipt waits and raw
request/sequence joins remain in the existing observer. Cross-clock alignment
requires separate evidence and is never assumed.

`analyze_kernel.py` checks the footer, every retained event count, syscall and
nested-call entry/return counts, successful results and interval containment.
It reports an explicit remainder after the union of nested intervals; nested
calls are not summed as disjoint wall time. Any tracer warning, missing record,
failed I/O, invalid host or application-observer overflow refuses complete
attribution. This is an instrumented diagnostic, not a new baseline or a
measurement of observer overhead or mitigation effect.

The fixed limits are a 900-second cell, 1,800-second task watchdog, 512 MiB
kernel log, four million kernel records, 100,000 sync calls, 4,096 map keys and
4,096 ring pages. The user granted continuing exclusive host use; the watchdog
is an execution bound, not an invented reservation expiry. Host isolation is
restored on success or failure. Existing helper files are transferred unchanged
from the capacity evidence directory and their digests are frozen in the private
input manifest. Binaries and raw logs remain private.

The bpftrace configuration follows its
[version 0.25 reference](https://bpftrace.org/docs/release_025/language).

## Retained failed attempt and repair

The initial collector attached successfully and passed 40 real sync calls.
During the workload, bpftrace aborted with `std::bad_alloc` at 54,353,065 log
bytes, below the log limit. The watchdog stopped the workload and restored
isolation. There is no complete cell, final trace footer or performance verdict.
The failed collector is retained as `initial-kernel-trace.bt`.

Its last subcall record precedes optional stack formatting. That makes stack
symbolication a suspected failure path, not a proved allocation root cause.
The replacement removes optional stack capture/formatting and keeps every named
native interval. Forty thousand real sync calls then pass all retention and
containment checks with empty stderr; nine parser tests cover incomplete and
inconsistent evidence. See `qualification.json`. The failed attempt is retained
separately and is never relabeled as a valid run.

## Reproduction

The collector requires admitted Linux hardware, exclusive host use, sudo,
bpftrace 0.25, the checked kernel probes, and the device-major check in the
runner. Qualification is a functional check only:

```bash
python3 -m unittest test_kernel
python3 analyze_kernel.py <kernel-log> <kernel-stderr> <output-json> --pid <load-pid>
```

The frozen protocol and analysis plan define the diagnostic. Results do not
by themselves close WP-810 or WP-749, and authorize no performance-selected retry.
