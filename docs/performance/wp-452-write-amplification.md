# WP-452 authoritative write amplification

Status: implementation and diagnostic evidence complete; the unchanged
`PERF-008` PostgreSQL parity gate remains open.

## Safety boundary

This package changes neither durable bytes nor command semantics. A successful
command still commits its mutation, outcome, events, provenance, audit,
idempotency terminal, and commit record atomically under the standard
Immediate one-phase durability profile. Transaction-current dependency and
admission checks remain inside the sole authoritative redb write transaction.

The storage backend now reuses the exact admission observation already proved
by the consuming `RedbCandidateAdmission` type-state. Terminal compare-and-set
checks still reject a missing, mismatched, or non-pending identity. It also
opens each entity, index, generation, event, route, and outbox table at most
once per transaction and does not open an empty mutation class. These are
byte-compatible reductions in redb tree lookup and table-handle work; they do
not remove a durable fact or a transaction-current proof.

## Evidence surfaces

The app-baseline report now separates three quantities:

- process write bytes from the operating-system process counter, normalized by
  every command committed by that measured daemon generation, including
  warmup;
- durable database-file growth over the same process generation; and
- a closed 13-table inventory captured after seed and after clean shutdown,
  with row, stored-byte, tree-height, leaf-page, branch-page, metadata-byte,
  and fragmentation values.

redb table pages attribute retained physical footprint. They are not presented
as per-table kernel write counters. Process write bytes remain the physical-I/O
total; the closed table inventory names which authoritative structures caused
that total and how much durable material each retained.

The additive fields are
`process_write_bytes_per_process_scope_committed_command` and
`durable_bytes_growth_per_process_scope_committed_command`. The older
`*_per_successful_mutation` fields remain byte-for-byte compatible in the v1
report for existing consumers, but mix a measured-window denominator with a
process-generation numerator and must not be used for write-amplification
comparison.

The command-growth harness additionally runs a 16-command standard-profile
synthetic group containing a complete command graph. It emits one
`write_amplification` record, a `table_inventory` record for every closed
table, and enforces a fixed ceiling of 65,536 allocated database bytes per
completed synthetic command. The numerator deliberately includes redb's fixed
database allocation, making the 32-command smoke gate conservative and
non-vacuous even when redb resizes its initial file.

## 2026-08-05 diagnostic attribution

A full-data append-only application probe completed 4,512 measured commands.
The same daemon committed 6,753 commands including warmup, in 423 physical
commits (mean group size 15.96). Correct process-scope normalization produced:

| Quantity | Total | Per committed command |
|---|---:|---:|
| process write bytes | 156,237,824 | 23,136 |
| durable file growth | 49,530,235 | 7,334 |

The retained table deltas for those 6,753 `CreateComment` commands identify
the dominant structures:

| Table | Row delta | Stored-byte delta | Leaf-page delta | Cause |
|---|---:|---:|---:|---|
| `commits` | 6,753 | 4,919,944 | 2,249 | authoritative command history |
| `idempotency` | 6,753 | 4,784,884 | 1,879 | persisted retry outcome |
| `provenance` | 6,753 | 3,320,590 | 1,240 | command attribution |
| `secondary_indexes` | 6,753 | 2,350,044 | 1,126 | comment lookup membership |
| `entities` | 6,753 | 2,200,371 | 956 | authoritative comment state |
| `audit` | 13,506 | 2,218,089 | 1,125 | linked Started and terminal facts |
| `audit_by_request` | 13,506 | 823,866 | 428 | request audit lookup |

`index_epochs` retained the same row count and changed by one stored byte in
this probe; it is not the current write-growth driver. `events`,
`event_routes`, and `outbox` were unchanged because this specific command emits
no domain event. Event-emitting workloads remain covered by the synthetic
complete-graph inventory.

The 32-command mechanics smoke recorded 20,480 allocated database bytes per
command against the fixed 65,536-byte ceiling. The ceiling is a regression
budget, not a parity claim and not permission to add durable records.

## Seed result and remaining ceiling

With the table-open and duplicate-admission-read reductions, one full
TicketDesk seed completed 19,220 commands in 3.568 seconds on the recorded
development host. Its writer decomposition was approximately 0.964 seconds in
validation/encoding/staging and 2.302 seconds in commit/flush across 356
physical groups. This is a diagnostic improvement from the recent roughly
3.8-second baseline, but it does not meet the under-three-second target and
does not satisfy `PERF-008` by itself.

The evidence now points to copy-on-write work across several required
authoritative trees plus durable commit cadence, rather than one accidental
table or a redundant audit scan. Further reduction that co-locates tables,
changes keys/pages, introduces a WAL, or serializes overlapping commands in
one transaction changes a compatibility or transaction boundary and belongs
behind explicit architecture review (WP-453), not a silent WP-452 shortcut.

## Page, cache, and layout experiments

No page-size, cache, or table-layout experiment is enabled in production.
redb 4.1 exposes page-size selection only to its own test/fuzzing builds, and
the service already uses its supported cache behavior. Any controlled engine
fork or future layout candidate must first prove crash/reopen, V1/V2 migration,
mixed-format compatibility, backup/restore, and the complete command graph.
Benchmark-only measurements must be labeled non-production until that evidence
exists.
