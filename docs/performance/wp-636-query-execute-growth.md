# WP-636 exact named-query execute attribution

WP-636 is a non-evidentiary diagnostic package. It does not amend the frozen
PERF-018 comparator, does not count removal of benchmark-driver time as a
product gain, and does not change query, integrity, authorization, freshness,
or transport semantics.

## Result

The suspected operation-count growth did not reproduce on the idle
workstation or E2. The workstation's fixed `GetTicket` path stayed at 394--410
microseconds from 500 to 10,000 samples; E2 improved slightly from 2.72 to
2.61 milliseconds. RSS rose by a bounded process-startup amount, not in
proportion to operations.

N1 produced one 10,000-sample outlier whose first 256-operation window was
already slow. It therefore was not gradual growth within the measured series.
The refined repeat found the bounded input responsible for between-cell
variation: the restarted server decodes whichever physical command segment
the concurrent seed happened to commit last. An 8-command/12.6 KiB tail cost
0.93 ms; a 24-command/37.7 KiB tail cost 2.57 ms. Both were flat by operation
ordinal. The original 11 ms artifact predates the tail-shape counter, so its
exact tail width cannot be recovered, but it is consistent with the same
mechanism rather than a retained per-read structure.

The stable and material finding is a regression of WP-481's constant-time
query-frontier lookup. After a process restart with no composite publication,
`RedbReadAccess::application_frontier` calls `read_commit_tail`. That path
opens the commit/event authorities and fully decodes and validates the latest
physical command segment merely to learn the logical application head. The
same segment is decoded again on every named read.

Commit `53d73087` changed production query execution from
`read_snapshot_head`, which reads the sequence allocator and checks authority
presence in constant time, to the composite-view frontier call. The existing
`snapshot_frontier_does_not_decode_retained_command_history` test exercises
the old helper rather than `QueryExecutionPort`, so it remained green after
the production caller stopped using that helper.

## Closed stage measurements

> WP-754 moved query execution out of the concrete storage backend. The
> storage-owned `riffdb-query-execute-windows-v1` census therefore emits no
> samples after that package; it does not publish zero-filled stages as if they
> measured executor work. Any later performance-evidence package must add an
> executor-owned, newly versioned census before collecting comparable timings.

The opt-in `RIFFDB_QUERY_EXECUTE_DIAGNOSTICS=1` census stores at most 64
windows of 256 operations. The terminal window absorbs excess operations. It
records only closed stage durations, composite overlay counts/bytes, and the
redaction-safe byte/cardinality shape of the physical authority tail. No
application value, key, principal, credential, source, or unbounded sample
series is retained.

Mean times below are from the frozen synchronous client shape. `other execute`
is all measured execute work other than frontier capture. `residual` is the
customer-paid generated/tonic/service time outside the storage execute span.

| host/cell | total | execute | frontier | other execute | residual | throughput |
|---|---:|---:|---:|---:|---:|---:|
| workstation, 500 | 394 us | 279 us | 270 us | 9 us | 115 us | 2,534/s |
| workstation, 10k | 410 us | 285 us | 277 us | 9 us | 124 us | 2,435/s |
| N1, 500 | 1,908 us | 1,347 us | 1,319 us | 28 us | 561 us | 522/s |
| N1, 10k outlier | 11,932 us | 11,096 us | 11,049 us | 47 us | 836 us | 83/s |
| N1 refined 500, 8-command tail | 1,501 us | 967 us | 934 us | 33 us | 534 us | 663/s |
| N1 refined 10k, 24-command tail | 3,222 us | 2,600 us | 2,567 us | 33 us | 622 us | 309/s |
| E2, 500 | 2,723 us | 1,941 us | 1,906 us | 35 us | 782 us | 366/s |
| E2, 10k | 2,608 us | 1,876 us | 1,842 us | 34 us | 732 us | 382/s |

For a comparable 8-command authority tail, dilation versus the workstation is
3.8x for N1 end to end and 3.5x for frontier capture. E2's unrefined 500 cell
is 6.9x/7.1x. The previously reported 9--10x N1 dilation is a whole-cell
authority-tail-width effect, not a stable per-operation slope.

The refined authority-tail probe observed a 12.6 KiB physical row containing
8 logical commands in both the workstation and the normal N1 repeat. A second
N1 seed ended on 37.7 KiB/24 commands; frontier time scaled by 2.75x as tail
bytes scaled by 2.98x. Tail shape explains the cell-to-cell anomaly and why
the hot symbols are command-record validation machinery rather than entity
decoding. It does not explain the same-shape 3.5x host dilation.

`/proc/PID/smaps_rollup` RSS deltas were +35.4 MiB/+36.4 MiB on the workstation
at 500/10k, +8.1 MiB/+37.0 MiB on E2, and +35.8 MiB/+16.1 MiB on the original
N1 cells. The refined N1 10k repeat added 36.0 MiB. These do not support a
retained object per read. Composite overlay transition and byte counts were
zero throughout the restarted read-only cells. One-second `vmstat` over the
refined N1 long cell reported 0% steal; its first and last frontier windows
were 2.54 and 2.52 ms.

## What `execute` is doing

The workstation server profile attributes the frontier walk to the expected
durable-record stack: `wire::Cursor::next`, durable preflight, table-driven
CRC32, prost varint encode/decode, SHA-256, and allocation/free. SHA-NI helps
the workstation while the N1 Skylake vCPU uses software SHA. CRC, varints, and
allocation are also cache/branch sensitive, which explains the above-hardware
cloud dilation without implicating transport.

The exact entity path after frontier capture is currently small: point lookup,
envelope identity/bounds, payload checksum, wire preflight, prost decode,
canonical re-encode, semantic reconstruction, target validation, row policy,
materialization, and query drive together cost about 9 microseconds on the
workstation. This measurement executes every current integrity and policy
check; the diagnostic does not skip work.

## Safe-application PostgreSQL target

The exact c=1 twin used the same full TicketDesk seed and the safe-application
PostgreSQL backend. PostgreSQL reported `synchronous_commit=on`, `fsync=on`,
`full_page_writes=on`, and `wal_sync_method=fdatasync`.

| 10,000 samples | mean | p50 | p99 | throughput |
|---|---:|---:|---:|---:|
| RiffDB generated `GetTicket` | 406 us | 410 us | - | 2,464/s |
| safe-app PostgreSQL `get_ticket` | 86 us | 82 us | 139 us | 11,642/s |

This twin is diagnostic, not release evidence. It supplies the target number
without changing either frozen comparator.

## c=32 CPU/off-CPU classification

The workstation c=32 fixed-read cell sustained 21,227 operations/s with a
1.50 ms mean generated call and a 298 us mean server execute span. During a
10-second capture, `riffdbd` consumed 39.99 core-seconds: all four configured
server worker cores. Publication locks measured below one microsecond and no
storage/read-view wait was material. The c=32 plateau is therefore CPU/runtime
queueing under saturation, not an HTTP/2, storage-mutex, or off-CPU lock-wait
ceiling.

## Predeclared candidate arithmetic

These bounds are declared before any candidate implementation.

1. **Restore the WP-481 constant-time frontier.** On workstation c=1, frontier
   capture is 68.6% of the generated call. Replacing it with the prior
   allocator plus constant-time authority-presence check and retaining the
   current 9 us of entity execution predicts roughly 155 us total, a 2.5x
   ceiling. Using host-scaled constant-time checks predicts approximately
   0.6--0.9 ms on the normal cloud cells. In the interactive mix, 85% of
   operations are reads; the c=32 profile places approximately 46% of server
   CPU in the durable decode stack. Removing 85% of that fraction gives an
   Amdahl ceiling of `1 / (1 - 0.46 * 0.85) = 1.64x`. The activation gate is at
   least +100% on c=1 fixed `GetTicket`, at least +35% on public interactive
   c=32 throughput, and no seed, write, correctness, or tail-latency regression.
2. **Validate-once for ordinary entity rows.** Excluding the frontier walk, all
   entity validation and assembly is at most 2.3% of workstation c=1. Even
   deleting it entirely yields only 1.02x. It is rejected as the next package.
   Validation-once is relevant only to the authority-frontier regression, and
   must preserve fail-closed activation/recovery validation plus a same-
   snapshot allocator/authority consistency check.
3. **Decoded-entity cache.** Point lookup through materialization is within the
   same approximately 9 us remainder, for a less-than-1.02x c=1 ceiling before
   cache lookup, invalidation, and memory cost. Rejected until a later profile
   makes it material.
4. **Lazy partial decode.** Prost decode itself is below one microsecond on the
   workstation entity path. Its perfect-removal ceiling is below 1.01x.
   Rejected.
5. **Growing retained structure.** No operation-index slope or proportional RSS
   growth was found on the workstation, E2, or refined N1 run. The anomalous
   N1 series was slow from its first window, and the refined counters identify
   seed-tail width as the bounded varying input. No separate product change is
   authorized for a retained-growth hypothesis.

The frontier repair belongs to a separate work package. It must exercise the
actual `QueryExecutionPort` after a restart and prove the hot path does not
decode command history; a helper-only regression test is insufficient.

## Reproduction and artifacts

Representative diagnostic commands:

```text
riffdb-client-transport-diagnostic --scale full --clients 1 \
  --samples-per-client 500 --warmup-per-client 32
riffdb-client-transport-diagnostic --scale full --clients 1 \
  --samples-per-client 10000 --warmup-per-client 32
```

The diagnostic also accepts `--postgres-url` for the safe-app twin. It remains
`perf_018_eligible=false` and `release_comparator_changed=false`.

Key receipt SHA-256 values:

- workstation 500: `f376cf651b25a96fdeed8bfa1b8656ff5965b207f7d830cc9c761fb16585dccf`
- workstation 10k: `e9316c30b6b38bfbcc45220f5da9e1e10cadf960cb6640018dff857f0e9f3095`
- workstation c32: `ea25af5968a2ab23b55062aeefedc112a0c53c81bf5a87b7180c0dbdaf42ae79`
- workstation c32 perf: `451dc5692fd87721ee994c80b308e67ef8b2a034a2163e8c51dc0237b0af436b`
- workstation safe-PG twin: `49cb2ce580eb34ead5fcf836b4e9f0833b339affe8c58327defe6a7f8e3c74e6`
- N1 500/10k: `1c876b938b41bf1148c804c453d879771d43424a340e6a37c5a1a93a14ce7044` / `9ddbe808ad754b8afe86e4696d20e22c33b036536fa08687ef9ae3f8ce54a3ac`
- N1 refined 500/10k: `3778d62118e16a6d9665a2590c7be24cbd47784428bb23a4b228de2ef39b162a` / `7c7c6a22ae12e410c37f0f2330a787b3f9ac897a9cf13262053d2dfb55d7a42f`
- N1 refined 10k VM telemetry: `3ad90e90d7352d829badbbc3018a1cc13b0908fca7cabe09fe250529a887eb45`
- E2 500/10k: `0e1ea65c24a9ad776327a1484cbc6ab05c2d026a192c3e55487dd59d88e63aac` / `e2289d2a0240e4ebed7a0b857f2ba2bc678156f64d34c9373030708247f0b8c9`

The supplied pre-WP-636 workstation artifacts were copied unchanged under
`~/tmp/wp636-input`; their hashes are recorded in the work-package handoff.
