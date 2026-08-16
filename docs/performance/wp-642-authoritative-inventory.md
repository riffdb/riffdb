# WP-642 post-measurement authoritative inventory reliability

Date: 2026-08-16

## Closed boundary

An app-baseline cell is correct only when all of the following complete in
order:

1. every measured public operation has a classified outcome;
2. the daemon performs its clean shutdown protocol and returns bounded writer,
   journal, frontier, and checkpoint evidence;
3. the stopped database is reopened once through `RedbStore::open`, including
   format preflight and checkpoint-plus-journal recovery;
4. the reopened handle is closed; and
5. one mutation-free redb read transaction returns the complete fixed catalog
   of 13 authoritative tables.

There is no retry and no partial-table result. Failure at any stage fails the
benchmark cell. The report records
`post_shutdown_structural_reopen: true` and names the inventory scope as the
post-shutdown structural reopen.

## Reproduction and diagnosis

The retained original failure completed 8,135 measured public writes without
an application error, then failed the mandatory epilogue with only
`engine benchmark operation failed`:

- `/home/user/tmp/wp469-write-only-escalated.log`
- SHA-256
  `fe21d6d1b9ba48cd2c3f1b2931755ce46cb1bdc75d5bb9f89ddc45b7f9d0a20f`

The old epilogue opened the redb checkpoint directly. That did not prove the
authoritative durable unit, which is the checkpoint plus its journal suffix.
It also collapsed open, transaction, table, statistics, and count failures
into one generic error.

The exact timing-dependent process failure did not recur in a ten-repetition
current-HEAD smoke run before the repair. That negative result is retained at
`/home/user/tmp/wp642-repro-current.json` (SHA-256
`58d0429d8f080ee220fc9c97dcbfe01f601bf62a4576f9f285a66bd1ae86310a`)
and is not represented as a deterministic natural reproduction.

Two deterministic storage tests close the mechanism instead:

- a live competing database owner makes the required structural reopen fail
  at the typed `reopen_database` stage; after that owner closes, the same path
  returns exactly 13 tables;
- an invalid journal leaves all 13 raw redb tables readable, proving that the
  former raw inventory could report a false green, while the repaired
  structural reopen fails closed at `reopen_database`.

Inventory diagnostics are a closed stage plus an optional fixed physical table
name: `reopen_database`, `open_database`, `begin_read`, `open_table`, `stats`,
or `count`. They contain no keys, values, paths, query text, or application
identifiers.

## Repaired receipts

All cells used the production public-gRPC app-baseline process with 32 clients,
a three-second measured interactive window, one-second warmup, and three
repetitions. These are reliability receipts, not performance evidence.

| Profile | Report SHA-256 | Repetitions | Structural reopens | Tables per repetition | Frontier failures | Public errors / unavailable |
|---|---|---:|---:|---:|---:|---:|
| Workstation | `cf08c9c23001ae2825c4774aad3df0a8dce667487337224c73e535eed2720365` | 3 | 3 | 13 | 0 | 0 / 0 |
| E2 cloud | `df6191b94d4780fbfbda40fa3981b44d16eb7964817c90488926dcf59c021602` | 3 | 3 | 13 | 0 | 0 / 0 |
| N1 cloud | `6954def7867a79df90dffbd329ff67955c2aaaf4074ecd653d5dfceb3b331568` | 3 | 3 | 13 | 0 | 0 / 0 |

Paths:

- workstation: `/home/user/tmp/wp642-fixed-workstation.json`;
- E2: `/home/user/tmp/wp642-fixed-e2-02.json` on `bench-host-e2`;
- N1: `/home/user/tmp/wp642-fixed-n1-02.json` on `bench-host-n1`.

The structural reopen is the journal/checkpoint closure proof. The subsequent
single read transaction supplies the exact table inventory, while the daemon's
shutdown evidence supplies fixed-cardinality journal frame/flush censuses and
the applied-frontier equivalence counter. No receipt is accepted from only one
of those views.

## Safety result

The repair changes only benchmark conformance evidence. It exposes no storage
handle and adds no application opt-out. A successful public workload followed
by an unprovable durable-unit reopen is still a failed cell; evidence quality
cannot be recovered by retrying until the database happens to open.
