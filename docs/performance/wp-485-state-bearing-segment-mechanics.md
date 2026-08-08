# WP-485 state-bearing segment mechanics

WP-485 is a negative mechanics result. It did not change production storage,
read authority, durable formats, or recovery semantics, and it did not produce
ADR-0104.

## Question and decision gate

ADR-0102 made a bounded command segment authoritative for successful command,
outcome, provenance, audit, event, route, and logical generation facts. Complete
entity post-images and secondary-index entries remain independently
authoritative redb rows.

WP-485 asked whether one state-bearing command segment should also own those
entity and index transitions while exact current-view indexes become derived.
The proposal is a substantial authority, recovery, compaction, and read
validation change. Before drafting that change, the benchmark required at
least **2.0 times lower median redb table work** for every distinct-create,
retained-update, and mixed workload at physical group sizes 32 and 128.

The benchmark is deliberately not an application benchmark. It uses
`Durability::None` for measured redb begin/stage/commit work, followed by one
common durability barrier so the resulting page inventory can be inspected.
This isolates table and page mechanics from the journal fence.

## Projection shapes

The current projection retains:

- one complete command segment per physical group;
- one final index-generation row per physical group;
- one complete entity post-image and secondary-index row per command;
- the group allocator post-image.

The benchmark-only proposal retains:

- one command segment per physical group containing the complete entity,
  secondary-index, event, and generation transitions;
- the group allocator post-image.

Independently mutable delivery-status rows remain separate in both models, but
the audited production command path derives initial pending-outbox membership
from the segment and therefore creates no such row in this hot-path probe.

The proposed segment is charged for the complete state bytes. The current
`StoredCommandCapsuleV2` contains entity references, not complete entity
post-images, so omitting those bytes would be an invalid comparison.

Each case completes 1,024 commands. Retained-update and mixed cases start with
4,096 current entities. Seven repetitions are reported by the checked run.

## Result

| workload | group | current table work | proposed table work | speedup | current/proposed leaf pages |
|---|---:|---:|---:|---:|---:|
| distinct creates | 32 | 4.70 ms | 2.63 ms | 1.79x | 504 / 33 |
| distinct creates | 128 | 4.03 ms | 2.47 ms | 1.64x | 479 / 9 |
| retained updates | 32 | 5.36 ms | 4.34 ms | 1.23x | 1,929 / 49 |
| retained updates | 128 | 4.91 ms | 4.55 ms | 1.08x | 1,903 / 25 |
| mixed create/update | 32 | 5.77 ms | 4.29 ms | 1.35x | 2,281 / 49 |
| mixed create/update | 128 | 5.07 ms | 4.36 ms | 1.16x | 2,255 / 25 |

The proposed/current table-work basis points were 5,594/6,111 for distinct
creates, 8,105/9,265 for retained updates, and 7,432/8,599 for mixed work at
groups 32/128. All six required cases exceed the maximum 5,000 basis points.
The decision gate therefore failed.

The proposal does reduce final leaf-page inventory by roughly 15.3x to 90.2x.
That does not translate into a 2x writer-time improvement. Large state-bearing
values move work into contiguous payload construction and redb overflow-page
handling, while the current grouped row shape is already relatively efficient.
The proposal retains about 4 percent more logical bytes for retained updates;
distinct creates and mixed work are within roughly one percent. Complete
historical state transitions replace compact current-state rows as history
grows even though the hot group writes fewer B-tree keys.

## Decision

Do not change entity or secondary-index authority on this evidence. In
particular:

- no production code may select the benchmark projection;
- `ENTITIES` and `SECONDARY_INDEXES` remain authoritative current-view tables;
- startup, transaction-current validation, queries, backups, and recovery keep
  their accepted ADR-0102 behavior; and
- ADR-0104 is not drafted merely because the page-count metric looks favorable.

A future investigation may test a different append-optimized immutable state
store or a smaller state-delta encoding, but it needs a new predeclared
mechanics gate. It must account for read lookup, bounded checkpoint rebuild,
retention, crash recovery, backup bytes, and actual application payload
distributions before proposing an authority change.

## Reproduce

Use an on-disk root:

~~~bash
RIFFDB_TMP_ROOT=$HOME/tmp TMPDIR=$HOME/tmp \
  cargo +1.97.0 run --release \
  --manifest-path benchmarks/command-growth/Cargo.toml \
  --bin state-segment-projection -- \
  --database-root target/perf-db/wp485
~~~

The harness emits riffdb.state-segment-projection/v1 JSONL with sample,
per-table inventory, median summary, and decision-gate records. A false
decision gate is a valid completed mechanics investigation; the process fails
only when measurement itself cannot complete.

Automated coverage exercises the closed projection shapes, absence of invented
delivery rows on the audited command path, group bounds, retained mutation
mixes, and report aggregation. This evidence covers `STO-002`, `STO-020`,
`STO-021`, `STO-022`, `PERF-004`, `PERF-005`, `PERF-008`, `PERF-016`, and
`PERF-017` without changing their production guarantees.
