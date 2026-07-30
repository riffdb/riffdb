# WP-366 TicketDesk write-parity evidence

Date: 2026-07-30

This note records the full same-run `app-baseline` result after the
same-partition aggregate, catalog-cache, grouped-audit, CRC, and bounded batch
changes. It is engineering evidence, not a publication benchmark.

## Safety boundary retained

The measured RiffDB path uses public symbolic commands and named RiffQL over
gRPC. Every command retains its own authorization, idempotency identity,
declared outcome, provenance, service-audit lifecycle, commit record, and
uncertainty recovery. Authoritative redb commits retain `Immediate` durability.

The batch transport contains at most 16 ordinary commands. It is neither one
transaction nor a bulk-storage escape hatch. Generated clients cap total
in-flight items at 64, the server caps both semantic-port and coordinator
admission at 64, and each compatible physical group remains capped at 64.

Exact same-partition external reads are transaction-current dependencies.
Grouped execution rejects every exact read/write or write/write overlap,
including the case where an earlier command reads an entity written by a later
command.

## Fixed amplification removed

- Active catalog snapshots are reused only while the complete durable
  lineage/version/bundle-hash pointer matches.
- Executable plans are reused only under the exact active pointer against
  which their lineage proof was validated.
- Projection catalog material is similarly invalidated by the durable active
  pointer.
- Terminal service-audit rows validate the administration tail and advance
  its allocator once per physical command group.
- CRC-32C uses the existing safe 16-lane implementation. The checksum
  algorithm and all durable bytes are unchanged.
- Generated command batches use the bounded public batch RPC and recover a
  failed or uncertain exchange through each original idempotency identity.
- The internal FIFO writer may form compatible physical groups up to the
  pre-existing 64-command transaction ceiling while public transport batches
  remain capped at 16.
- A conflict splits one actor selection into maximal FIFO-compatible
  subgroups; it no longer forces unrelated commands in the same selection
  through individual completion commits.
- The application sequence allocator is staged once at the end of each
  physical command transaction instead of being overwritten for every command.
- Current writable generated Protobuf messages use a sealed schema binding and
  the existing allocation-free structural preflight. Untyped payloads retain
  strict decode/re-encode validation, while typed writers avoid redundantly
  decoding bytes they just produced. Durable bytes are unchanged.

## Full same-run result

Source: `target/app-baseline/report-v1.json`

| Metric | PostgreSQL | RiffDB | RiffDB/PostgreSQL |
|---|---:|---:|---:|
| Seed, 15,160 items | 3,815.0 ms | 5,977.7 ms | 1.57× |
| `create_comment` | 4.644 ms | 1.887 ms | 0.41× |
| `close_ticket_with_comment` | 7.920 ms | 1.047 ms | 0.13× |
| `swap_member_roles` | 7.906 ms | 0.995 ms | 0.13× |
| `open_ticket_with_labels` | 8.044 ms | 1.392 ms | 0.17× |
| `ticket_detail_page` | 8.419 ms | 1.402 ms | 0.17× |

Every measured interactive read and write beats PostgreSQL in this run.
`open_ticket_with_labels`, previously three public command RPCs, is now one
atomic compiled command.

This same-run evidence was collected under visible host contention, which is
why both absolute seed values are slower than the earlier run. The gate is
deliberately a same-run ratio. RiffDB passed it while every interactive read
and write remained faster than PostgreSQL.

## Gate result

The ADR-0059 seed gate is at most 2× the same-run PostgreSQL duration. This run
is 1.57×. Representative unary mutation p50 is 0.41×, so both PERF-005
write-parity gates pass.

RiffDB still performs stronger per-item work than the PostgreSQL seed: 15,160
independently authorized and recoverable commands, each with a durable outcome,
provenance, commit, and two-phase service-audit evidence. This run completed
15,164 measured command completions in 1,921 physical completion commits, an
average group size of 7.89. The fixed-cardinality report covers every size from
1 through 64. Continued optimization must preserve these guarantees,
`Immediate` durability, and the public 16-command transport bound.

## Reproduction

```bash
./benchmarks/run-app-baseline --full --assert-write-parity
```

The report includes the fixed-cardinality physical completion-group
distribution for group sizes 1 through 64. The command intentionally exits
nonzero while either the seed or any interactive write p50 exceeds 2× the
same-run PostgreSQL value.
