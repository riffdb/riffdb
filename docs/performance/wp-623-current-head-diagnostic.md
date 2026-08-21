# WP-623 current-HEAD failed-gate diagnostic

Date: 21 August 2026

Status: diagnostic closure for release candidate
`29434655e7ec93a6e90414f806111f82d60a6ec3`. This is not PERF-018 release
evidence. The three-repetition qualification was stopped after the first
complete N1 and E2 repetition because the unchanged candidate could not pass
the conjunctive gates; spending another twelve measured minutes could not
change that result.

## Gate result

The first complete cloud repetition used the frozen unary generated client,
safe-application PostgreSQL, 90 measured seconds after warmup, and the exact
WP-623 candidate digest.

| Profile | c32 safe PG | c32 RiffDB | throughput ratio | p95 ratio | Result |
|---|---:|---:|---:|---:|---|
| N1 | 8,222 ops/s | 7,463 ops/s | 0.908x | 1.379x | throughput pass; tail fail |
| E2 | 8,920 ops/s | 7,671 ops/s | 0.860x | 1.333x | throughput and tail fail |

Every measured operation completed without a correctness, conflict, or
idempotency mismatch. The workstation refused evidence before measurement
because unrelated container and JavaScript processes violated the idle-host
inventory. No partial cloud or interfered workstation receipt is promoted.

At c1, ordinary RiffDB point reads were 0.623 ms on N1 and 1.114 ms on E2,
versus 0.279 ms and 0.328 ms for safe PostgreSQL. Representative writes also
miss: `CreateComment` was 3.277/6.029 ms against 2.490/3.015 ms. The failure is
therefore not confined to the c32 queue or the large-result page.

## Small-read transport closure

The non-evidentiary `riffdb-client-transport-diagnostic` ran 64 warmups and 500
measured generated `GetTicket` calls at c1. `TCP_NODELAY` remained enabled and
the synchronous bridge cost stayed a few microseconds.

| Profile | public mean | public p50 | measured server mean | customer-paid residual |
|---|---:|---:|---:|---:|
| N1 | 0.573 ms | 0.590 ms | 0.141 ms | 0.432 ms |
| E2 | 0.722 ms | 0.688 ms | 0.161 ms | 0.561 ms |

The authoritative named-query subledger is only about 43--56 microseconds per
point lookup. Perfect deletion of service authentication and dispatch cannot
recover the 0.28--0.33 ms required by the 1.10x point-read gate. The remaining
mechanism is the real Tonic/HTTP2/client scheduling path, not entity storage,
RiffQL parsing, derived freshness, or the synchronous benchmark bridge.

ADR-0127's existing multiplexed implementation is not sufficient: WP-650
measured it 2.7% slower than unary on N1 and 15.5% slower on E2. Its client path
adds an outbound channel, mutexed `BTreeMap`, per-call oneshot, response-routing
task, and cancellation bookkeeping even when one closed-loop caller has only
one operation in flight. A follow-on may optimize the already accepted session
semantics, but it must first prove a materially cheaper single-owner mechanics
path. It may not cache authority, imply ordering or freshness, or change the
frozen comparator merely to improve a benchmark.

## Current V14 BoardPage ledger

A second diagnostic limited measurement to compiled `BoardPage50/200/450`,
with 20 warmups and 100 samples per size. The server's fixed-cardinality
read-stage and query-execute telemetry was enabled.

| Profile | 50 rows | 200 rows | 450 rows | executor mean over all sizes | adapter conversion mean |
|---|---:|---:|---:|---:|---:|
| N1 | 1.740 ms | 2.787 ms | 4.752 ms | 1.369 ms | 0.179 ms |
| E2 | 2.292 ms | 3.110 ms | 4.775 ms | 1.346 ms | 0.137 ms |

The final query-execute window contains seven 200-row calls and one hundred
450-row calls. Its `program_drive_exclusive` mean is 1.295 ms on N1 and 1.367
ms on E2. Thus the covered positional executor is no longer the dominant
450-row cost. Roughly three milliseconds remains in generic `Value` creation,
Protobuf framing/decoding, generated positional decoding, row construction,
HTTP2, and scheduling.

## Packed-carriage existence proof

The same process generation also checks the projected row and packed arms for
byte-equivalent application values before timing them. Both shapes contain the
same 450 rows and six application cells per row. The provider's roughly
52--63 ms fixed cost dominates both calls, so the paired difference isolates
the existing packed column-major wire/client mechanism without relying on the
projected provider as a performance candidate.

| Profile | Rows | projected row | projected packed | packed saving |
|---|---:|---:|---:|---:|
| N1 | 50 | 63.010 ms | 62.823 ms | 0.187 ms |
| N1 | 200 | 63.857 ms | 62.789 ms | 1.068 ms |
| N1 | 450 | 65.512 ms | 63.216 ms | 2.296 ms |
| E2 | 50 | 52.734 ms | 52.394 ms | 0.340 ms |
| E2 | 200 | 53.362 ms | 52.539 ms | 0.823 ms |
| E2 | 450 | 54.508 ms | 52.546 ms | 1.962 ms |

Applying only the observed 450-row saving to the compiled V14 page predicts
2.46 ms on N1 and 2.81 ms on E2. The paired safe-application PostgreSQL target
from WP-654 is approximately 2.78 ms on each host after the 1.10 multiplier.
The E2 prediction is deliberately treated as threshold-close rather than a
pass claim. It nevertheless clears the mechanics bar for a reject-first
candidate that reuses the already implemented canonical-cell `PackedColumn`
codec and adds no new value format.

## Decision

WP-623 cannot qualify current HEAD and remains open. Two material candidates
are justified, with separate activation and rollback:

1. extend compiler-selected compact named results with a negotiated packed
   canonical-column arm decoded directly by generated clients; and
2. replace the existing session's per-call multiplexing machinery with a
   bounded single-owner fast path for the common one-in-flight case while
   retaining the accepted multiplexed path when concurrency actually requires
   it.

Neither candidate changes commands, query semantics, authorization safe
points, durability, freshness, idempotency, result bounds, or caller control.
Packed carriage remains compiler-selected and identity-checked. Session work
remains an optional transport implementation until it independently passes
ADR-0127's semantic and public gates and a human accepts any PERF-018
comparator amendment.

## Receipts

| Receipt | SHA-256 |
|---|---|
| N1 90-second safe-PG repetition | `bcb6c273b75da3d64397ab367c0a7f40016e4faf4caa584c76bec37f412d7c9b` |
| N1 90-second RiffDB repetition | `fb91934339193a2b9b4977e27eee6ddb9de16f79d439d0acda3c7fbb25720ae8` |
| E2 90-second safe-PG repetition | `e9e3d4cdaf71ebc4d261bd8780069e9129bd092f592ac0469cbe6861020358b6` |
| E2 90-second RiffDB repetition | `d63c155d6bcc85f8e97055bd32d6e38ad10e9c3eee86ebd303e9e31a23b69b5b` |
| workstation interference refusal | `b1b6c718ed788b11429a7ef12ee1bf68dffa4ea350fca4fc022ae588872e0726` |
| N1 point transport diagnostic | `e637fe4d3a048b3a5d6d3e8236f66862a9074ab0f9c7a8c5d61ee98c1d9a5ea1` |
| E2 point transport diagnostic | `2dae0e84cb7588b69271a23cea7fad775d616f907fdb2deb36c6e7c0de720ec3` |
| N1 compiled-board diagnostic | `5c804c49f2a2b7397a4891ef17b5d9d6b22ec95cb293e74683b48a2f9d7e78d9` |
| E2 compiled-board diagnostic | `650c174554ffd112447b7f0005f71283326e929705ff441232520f6dd5a6e33e` |

Raw receipts remain outside the repository under `/home/kevin/tmp`. They
contain no credentials or application values.
