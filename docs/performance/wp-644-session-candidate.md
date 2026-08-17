# WP-644 bounded application-session candidate

WP-644 implemented ADR-0127's optional bounded multiplexed transport for
generated commands and exact named queries. The candidate is useful, but it is
**not activated as the default transport**: it missed the reject-first N1
activation gates. Unary gRPC remains the generated-client default and these
short cells do not amend or qualify `PERF-018` release evidence.

The candidate changes transport only. Each envelope re-enters the ordinary
gRPC adaptation and API-neutral application service, including fresh
authentication and authorization, exact command/query resolution,
idempotency, durability, uncertainty, and explicit read-after-commit. It adds
no session transaction, snapshot, implicit ordering, authority cache, storage
access, or automatic fallback.

## Protocol and lifecycle result

- One additive versioned bidirectional RPC binds one database, credential
  presentation, exact contract, selected query modules, and reviewed
  application-lock hash.
- Correlation IDs are nonzero and strictly increasing. Completion may be out
  of order; duplicate or unknown responses close the client fail-closed.
- The negotiated in-flight ceiling is at most 128. In-flight work,
  cancellation tombstones, output buffering, total lifetime work, session
  duration, and output stalls are all finite.
- Cancelling a query yields a typed cancelled operation. Cancelling a command
  remains outcome-unknown unless ordinary idempotency recovery proves its
  result. Stream loss retains the same rule.
- Generated Rust clients select the session explicitly. Metadata drift is
  rejected locally, stream loss releases pending calls, and reconnect opens a
  new session generation. Unary remains available side by side.
- The process test performs an original generated write, an explicit
  generated read fenced by that commit, reconnects and reads again, then kills
  the server under session load and requires bounded abort.

Strict public-message preflight validates nested session envelopes before
Prost can merge duplicate singular fields. The server dispatch is pinned by an
architecture test to the ordinary command and application-query handlers and
the retained metadata type has a redacting `Debug` representation.

## Short cloud candidate result

These are two 10-second diagnostic repetitions after two seconds of warmup.
They are intentionally non-evidentiary. The mixed cells used measurement
snapshot `8b81fe05...`; later fail-closed correlation ordering and explicit
controlled-close repairs can invalidate a positive activation claim, but
cannot rescue the rejected result below. The dedicated cells were rerun from
the later `a852a1d7...` snapshot after those repairs.

| host | clients | safe PG ops/s | session ops/s | vs unary baseline | session / PG | PG p95 | session p95 | p95 ratio | gate result |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| N1 | 1 | 1,650 | 1,158 | +29.0% | 0.70x | 2.16 ms | 2.82 ms | 1.30x | c1 gain pass |
| N1 | 8 | 7,731 | 5,870 | +22.1% | 0.76x | 3.60 ms | 5.37 ms | 1.49x | c8 gain pass |
| N1 | 32 | 8,322 | 8,379 | +16.0% | 1.01x | 14.68 ms | 20.45 ms | **1.39x** | **p95 fail** |
| N1 | 128 | 7,370 | 8,901 | +15.5% | 1.21x | 71.30 ms | 83.89 ms | 1.18x | no regression |
| E2 | 1 | 870 | 711 | +21.5% | 0.82x | 4.39 ms | 4.46 ms | 1.01x | c1 gain pass |
| E2 | 8 | 6,893 | 4,739 | +11.7% | 0.69x | 4.46 ms | 6.82 ms | 1.53x | c8 gain pass |
| E2 | 32 | 8,038 | 7,568 | +12.7% | 0.94x | 16.25 ms | 20.97 ms | **1.29x** | **p95 fail** |
| E2 | 128 | 7,153 | 8,677 | +30.5% | 1.21x | 77.59 ms | 81.79 ms | 1.05x | no regression |

The session clears the predeclared c1/c8 throughput gains and c32 throughput
ratio on both hosts. It misses the c32 tail gate on both hosts: N1 requires at
most 18.35 ms and measured 20.45 ms; E2 requires at most 20.32 ms and measured
20.97 ms.

The activation decision is conjunctive. A gain on one host or one concurrency
level cannot activate the transport when another fixed cell misses. The
candidate therefore remains an explicit alpha diagnostic even though it
materially improves several public cells.

## Dedicated scenarios and seed

The dedicated cells use two repetitions of 100 measured samples. The public
seed remains on the unchanged bounded unary batch path; its measurement is a
regression guard, not a claim that the session accelerates seed.

| scenario | N1 PG p50 | N1 session p50 | ratio | E2 PG p50 | E2 session p50 | ratio |
|---|---:|---:|---:|---:|---:|---:|
| point ticket | 0.372 ms | 0.610 ms | 1.64x | 0.845 ms | 1.029 ms | 1.22x |
| point user | 0.283 ms | 0.515 ms | 1.82x | 0.462 ms | 0.943 ms | 2.04x |
| tickets by project/status | 0.336 ms | 0.728 ms | 2.17x | 0.486 ms | 1.136 ms | 2.34x |
| open tickets by assignee | 0.435 ms | 1.033 ms | 2.37x | 0.612 ms | 1.452 ms | 2.37x |
| comments for ticket | 0.332 ms | 0.575 ms | 1.73x | 0.525 ms | 1.002 ms | 1.91x |
| project members | 0.308 ms | 0.562 ms | 1.82x | 0.476 ms | 0.976 ms | 2.05x |
| ticket detail | 0.617 ms | 0.797 ms | 1.29x | 0.987 ms | 1.218 ms | 1.23x |
| board 50 | 1.858 ms | 1.879 ms | 1.01x | 1.655 ms | 2.177 ms | 1.32x |
| board 200 | 4.521 ms | 4.291 ms | 0.95x | 3.639 ms | 4.308 ms | 1.18x |
| board 450 | 2.779 ms | 8.227 ms | 2.96x | 2.972 ms | 7.775 ms | 2.62x |
| create comment | 2.536 ms | 3.387 ms | 1.34x | 4.686 ms | 4.901 ms | 1.05x |
| close with comment | 2.553 ms | 3.179 ms | 1.25x | 4.666 ms | 4.596 ms | 0.99x |
| swap member roles | 2.240 ms | 2.764 ms | 1.23x | 3.946 ms | 4.187 ms | 1.06x |
| open with labels | 2.396 ms | 3.377 ms | 1.41x | 4.330 ms | 4.824 ms | 1.11x |

No representative row reaches the required `<=1.10x` on both hosts. The
unchanged unary seed path remains inside the alpha ceiling: N1 measured
7.333 seconds versus 1.634 seconds (4.49x), and E2 measured 6.252 seconds
versus 2.775 seconds (2.27x). Both are below 5.0x; absolute RiffDB seed time
does not regress from the frozen unary baseline.

## Activation decision

**Rejected for default activation.** The bounded session remains a useful
explicit diagnostic because it materially raises mixed throughput, but the
conjunctive activation set fails on both c32 tails and every cross-host
dedicated scenario row. Unary therefore remains the generated-client default,
and this package makes no `PERF-018` release-evidence claim.

The first dedicated rerun also found a lifecycle defect: a live request stream
prevented the colocated daemon from completing graceful shutdown. The exact
failure reproduced on N1 and E2. The final client exposes an explicit close,
generated clients carry it, pending work is released under ordinary
uncertainty rules, and the harness closes before controlled daemon shutdown.
The repaired N1 and E2 reruns both exit zero. The failed logs remain receipts.

Because the activation set did not pass in full:

1. `PERF-018` and its comparator freeze are unchanged.
2. Generated clients continue to use unary unless an application explicitly
   opens the optional session.
3. No automatic fallback is introduced after a session opens; uncertainty and
   transport selection remain visible.
4. Python and TypeScript keep the semantically equivalent unary path. Their
   session comparators remain deferred as directed.

## Receipts

| receipt | SHA-256 |
|---|---|
| mixed measurement snapshot | `8b81fe05c8fb8ac5aa2d801eb20f0b9387a36feb9d1b3f86adcf29db75830f4a` |
| controlled-close snapshot | `a852a1d783d0b8fd1acfcbdbfe9a3ce220c69bca132766fd3682859caedfea53` |
| N1 mixed report | `e9ba8553e04779f88a822ed5dfa056d1bea7dd638160289352da5784242f3d20` |
| N1 mixed log | `014d961e41cf651cc673ff979c9df184cbab95f70cf9441efc865e1fc952b52c` |
| E2 mixed report | `b85b826512a343231abe7bcd7ace6bb2fe9bf9bec78a803f5bde8ef76e73abb7` |
| E2 mixed log | `551543c422021215659a5d76b24e37dd7eae41b8190244d1912d49d883fe965a` |
| N1 pre-fix graceful-shutdown failure | `1f86b526ec938b0115067ebabfe6510da46f7f06f8738e04f34d84a1f32ff6a6` |
| E2 pre-fix graceful-shutdown failure | `1a8d4ae166b64d426fbe1cc96b2209e15caca53708ceed12df66e59f419ed6d0` |
| N1 repaired dedicated report | `9d1d96e7b6c82e4fb74d0c1f9b3301d38de1e0a8059172ecc1f2e1477d9e87ce` |
| N1 repaired dedicated log | `ae515ccc303b8be7d3d62fd8339f2719afab53482eac31c3bc2041ebe80082a1` |
| E2 repaired dedicated report | `9e235c28baf44bfb33e00702a2952fbe516e2d9ce1b49fe93b63819a9bce4848` |
| E2 repaired dedicated log | `91fcca48aaa8437baee1347fe512a2d113cc2554e3b7a1907f2607cd52c479bc` |

The preceding unary baseline and predeclared decision arithmetic are recorded
in [WP-644 bounded-session baseline](wp-644-session-baseline.md).
