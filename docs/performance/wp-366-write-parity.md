# WP-366 TicketDesk write-parity evidence

Date: 2026-07-29

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
admission at 64, and each compatible physical group remains capped at 16.

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

## Full same-run result

Source: `target/app-baseline/report-v1.json`

| Metric | PostgreSQL | RiffDB | RiffDB/PostgreSQL |
|---|---:|---:|---:|
| Seed, 15,160 items | 1,480.5 ms | 3,909.0 ms | 2.64× |
| `create_comment` | 2.990 ms | 0.577 ms | 0.19× |
| `close_ticket_with_comment` | 4.998 ms | 0.547 ms | 0.11× |
| `swap_member_roles` | 4.684 ms | 0.527 ms | 0.11× |
| `open_ticket_with_labels` | 3.138 ms | 0.572 ms | 0.18× |
| `ticket_detail_page` | 3.834 ms | 0.496 ms | 0.13× |

Every measured interactive read and write beats PostgreSQL in this run.
`open_ticket_with_labels`, previously three public command RPCs, is now one
atomic compiled command.

The seed improved from the reported 17.5 seconds (about 867 operations/second)
to 3.91 seconds (about 3,879 operations/second): 4.5 times lower wall time.

## Open gate

The ADR-0059 seed gate is at most 2× the same-run PostgreSQL duration. This run
is 2.64×, so that gate remains open.

The seed comparison is deliberately demanding but not semantically symmetric:
PostgreSQL executes 15,160 row inserts inside one transaction and commits once.
RiffDB executes 15,160 independently authorized and recoverable commands, each
with durable outcome, provenance, commit, and two-phase service-audit evidence.
The run completed those commands in 1,147 physical groups (13.2 commands per
group). Even perfect 16-item grouping requires at least 948 `Started` plus
`Pending` commits and 948 command-graph plus terminal-audit commits. The
remaining gap is therefore dominated by the accepted two-transition and
16-command physical-group bounds, plus per-item canonical safety-record cost.
It must not be closed by removing those guarantees, weakening `Immediate`
durability, or introducing a direct import path.

## Reproduction

```bash
./benchmarks/run-app-baseline --full --assert-write-parity
```

The report includes the fixed-cardinality physical completion-group
distribution for group sizes 1 through 16. The command intentionally exits
nonzero while either the seed or any interactive write p50 exceeds 2× the
same-run PostgreSQL value.
