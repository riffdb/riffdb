# WP-486 journal-authoritative state overlay

WP-486 is a benchmark and architecture gate. It does not change production
storage, reads, commands, recovery, backup, or public behavior.

## Measured problem

The proxy-free 32-client application comparison measured 23,762 operations per
second through RiffDB and 46,204 through PostgreSQL safe-app. The corresponding
write-only slices measured 4,732 and 11,255 operations per second.

RiffDB's write-only path caused about 71 KiB of process writes per successful
mutation. Its interactive path caused about 110 KiB, compared with about 2 KiB
of PostgreSQL WAL per successful mutation. The preallocated durability-journal
fence is about 0.9 ms on this host, while redb application and command staging
consume roughly 2 ms per physical write group.

The current path therefore spends most of the remaining write budget applying
the complete authoritative mutation graph to redb before fencing a journal that
already contains the exact same mutations.

## Candidate

ADR-0101 already defines the standard-profile database as a durable redb
checkpoint plus its exact journal suffix. WP-486 measures whether RiffDB can:

1. build and fence the existing authoritative journal mutations;
2. publish a bounded immutable materialized overlay for that suffix;
3. serve one frozen read view by merging the overlay with the checkpoint; and
4. apply the suffix to redb asynchronously as a bounded checkpoint.

This is not available behavior. Proposed ADR-0104 contains the authority,
merge, backpressure, checkpoint, recovery, and backup rules under review.

## Predeclared gate

Production work proceeds only if benchmark-only evidence shows:

- at least 2.0-times lower pre-acknowledgement apply work at group sizes 32 and
  128 for distinct creates, retained updates, and a mixed workload;
- merged point and bounded index-page reads within 1.25 times the checkpoint-
  only control at the maximum overlay bound; and
- one bounded checkpoint can clear the maximum 4,096-transition suffix.

The benchmark must charge exact journal mutation bytes, overlay values and
tombstones, merge work, checkpoint work, retained bytes, and process writes.
It may not select the candidate through a production adapter.

## Result

Five repetitions per case passed the predeclared mechanics gate. The table
reports medians; pre-ack work is exact journal-frame construction plus either
redb non-durable apply or overlay apply. The common positional journal write and
durability fence are deliberately excluded from both sides.

| workload | physical group | current pre-ack | overlay pre-ack | speedup |
|---|---:|---:|---:|---:|
| distinct creates | 32 | 7.62 ms | 2.57 ms | 2.97x |
| distinct creates | 128 | 6.59 ms | 2.63 ms | 2.50x |
| retained updates | 32 | 7.93 ms | 3.07 ms | 2.59x |
| retained updates | 128 | 7.07 ms | 3.07 ms | 2.30x |
| mixed | 32 | 7.85 ms | 2.79 ms | 2.82x |
| mixed | 128 | 6.71 ms | 2.76 ms | 2.43x |

The overlay-apply interval initiated zero process writes in every median case;
this excludes the common journal bytes. Current redb apply initiated about
6.3--6.5 MiB for each 1,024-command case. Applying the complete 4,096-command
mixed suffix in one immediate redb checkpoint took 27.43 ms and initiated
23.56 MiB of process writes.

At the 4,096-transition bound, overlay point reads were 0.38 times the redb-only
control because every selected key hit the overlay. The bounded 50-row ordered
merge was 1.11 times the redb-only control, below the 1.25 limit. Semantic
checksums matched for every point and page case.

The maximum-overlay pre-ack construction was 1.80 times faster rather than 2
times faster. That case does not govern the 32/128 group gate, but it is useful
design evidence: checkpoint admission must start before the hard suffix bound,
and the production overlay should avoid repeated general-purpose tree-node
allocation as it grows.

The retained evidence is
`target/app-baseline/wp486-journal-overlay.jsonl`. Reproduce it with:

~~~bash
RIFFDB_TMP_ROOT=$HOME/tmp TMPDIR=$HOME/tmp \
  cargo +1.97.0 run --release \
  --manifest-path benchmarks/command-growth/Cargo.toml \
  --bin journal-state-overlay -- \
  --database-root target/perf-db/wp486
~~~

## Current status

The mechanics gate passed, and ADR-0104 received explicit exact-text approval
on 2026-08-08. Production remains on the apply-to-redb-before-fence path until
WP-487 through WP-490 complete the composite-view implementation and gates.

Implementation is decomposed into four gates:

1. WP-487 adds frozen checkpoint-plus-overlay read views and exact bounded
   point/scan semantics without selecting them for production writes.
2. WP-488 moves standard command and service-audit staging to frame-first
   overlay publication while retaining the hardened redb oracle.
3. WP-489 adds asynchronous checkpoints, deterministic restart rebuild,
   redb-writing barriers, backup/restore, reclamation, and process crash proof.
4. WP-490 enables the production composition and requires RiffDB's arithmetic
   mean across c1/c8/c32/c128 to beat same-run PostgreSQL safe-app, with no
   individual point below 0.90 parity.

The ADR audit also identified explicit specification amendments: `PERF-015`
and `PERF-017` previously required a published redb snapshot and recovery-time
checkpoint before readiness. ADR-0104 states the exact composite-view and
deterministic overlay-rebuild replacements, now encoded in `SPEC.md`.
