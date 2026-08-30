# Checkout comparison

Evidence for [ADR-0170](../../adr/0170-partition-local-cross-aggregate-writes.md).

`RDB-C017` refuses a command whose writes span two aggregate roots, so an
e-commerce checkout that decrements inventory and writes an order must either
place both entities under one root — accepting that root's coarser
`conflict_key` — or split into two commands and a compensating action. This
measures both, against a Postgres control that does the same work in one
transaction.

| directory | contract | conflict key | commits per checkout |
|---|---|---|---:|
| `atomic/` | one aggregate rooted at `Tenant` | `(tenant_id)` | 1 |
| `saga/` | `ProductData` and `OrderData` | `(tenant_id, product_id)`, `(tenant_id, order_id)` | 2 |
| `fine-atomic/` | `ProductData` and `OrderData`, one command | `(tenant_id, product_id)`, `(tenant_id, order_id)` | 1 |
| `postgres/` | tables with foreign keys | row locks | 1 |

`fine-atomic/` was unexpressible before ADR-0170. It is the shape the choice
above used to exclude: fine conflict keys *and* a single atomic commit.

The workload is identical in all three: a three-line checkout that decrements
three products and writes one order with three lines. Each concurrent client
owns a **disjoint** product triple, so under the saga's fine-grained conflict
keys the clients contend on nothing. That is deliberate — it gives the fine key
its best case.

## Results

Workstation, release daemon, one tenant, throughput in operations per second.
Postgres ran `fsync=on`, `synchronous_commit=on`, on the same NVMe device as
the RiffDB data.

Every run starts from an empty database. Three runs per RiffDB cell; three
Postgres runs, each preceded by re-applying `schema.sql`, for the reason under
"A confound worth avoiding" below. Median shown, individual runs in brackets.

| | c=1 | c=8 | c=32 | scaling |
|---|---:|---:|---:|---:|
| `atomic` | **483** [492, 483, 478] | **2377** [2377, 2460, 2349] | **5259** [5245, 5259, 5265] | 10.9× |
| `saga` | **244** [244, 244] | **1380** [1365, 1380] | **2559** [2523, 2594] | 10.5× |
| `fine-atomic` | **485** [489, 480] | **2629** [2675, 2583] | **5740** [5401, 6078] | 11.8× |
| `postgres` | **599** [607, 599, 592] | **2983** [3012, 2062, 2983] | **5833** [5974, 5753, 5833] | 9.7× |

p50 latency at c=32: atomic 5.6 ms, saga 9.9 ms, Postgres 5.1 ms.

**The fine-grained conflict key bought no measured scalability.** Clients
sharing one coarse conflict domain scaled as well as clients sharing none —
10.9× against 10.5× — because group commit amortizes the flush across
concurrent writers whether or not they contend on a lease. Splitting the write
cost roughly 2× throughput at every concurrency and doubled p50 latency.

**Fine conflict keys and one commit is the best shape measured.** `fine-atomic`
recovers the whole ~2× the split was costing (485 against the saga's 244 at
c=1, 5740 against 2559 at c=32) while keeping the per-product conflict key the
saga was paying for. It matches or slightly beats the coarse-key `atomic`
contract at every concurrency, which is the expected result once the premise
that a coarse key serialises writers has been refuted: the coarse key was never
costing anything, so removing it wins nothing, and the second commit was
costing everything.

**Postgres is ahead of the single-aggregate contract by 11–26%**: 1.24× at
c=1, 1.26× at c=8, 1.11× at c=32. That is consistent with the two other
durable-write comparisons in this repository — 1.18× on a single-transaction
order insert, 1.26× on the OpenFGA conformance suite — so the honest summary is
that RiffDB trails Postgres modestly and consistently on durable write
throughput, and that the aggregate split doubles that gap. Against
`fine-atomic` the remaining gap narrows to 7–18%.

## A confound worth avoiding

An earlier version of this measurement reported parity with Postgres. It was
wrong in Postgres's disfavour: each RiffDB run gets a fresh database from
`riffdb-dev`, while the Postgres control accumulated orders across runs. Once
`schema.sql` is re-applied before each run, Postgres gains 15–30% and the
parity claim disappears.

The RiffDB-against-RiffDB comparison is unaffected, because both contracts
always start empty. That is the comparison this example exists for.

## Reproducing

Ratios do not port between hosts — this workstation has a slow disk, and these
numbers are flush-bound. Re-measure rather than quoting these.

Generate the seeds (96 products, enough for 32 clients × 3 lines):

```sh
python3 gen_seeds.py
```

Run each RiffDB contract. `--acceptance` is required: it builds a **release**
daemon, and a debug build reorders the stage costs rather than merely scaling
them.

```sh
../../scripts/riffdb-dev --application-root "$PWD/atomic" --acceptance
../../scripts/riffdb-dev --application-root "$PWD/saga" --acceptance
../../scripts/riffdb-dev --application-root "$PWD/fine-atomic" --acceptance
```

Each run prints three `BENCH` lines on stderr, one per concurrency.

For the Postgres control, start a server and apply `postgres/schema.sql`.
Re-apply it before every run: it drops and recreates the tables, and a control
carrying rows from a previous run is 15–30% slower than a fresh RiffDB
database.

```sh
podman run -d --name checkout-bench -e POSTGRES_PASSWORD=bench \
    -e POSTGRES_DB=shop -p 45432:5432 docker.io/library/postgres:18
psql -h 127.0.0.1 -p 45432 -U postgres -d shop -f postgres/schema.sql
cargo run --manifest-path postgres/Cargo.toml
```

Put the Postgres data directory on the same device as the RiffDB work root. The
first version of this measurement had them on different NVMe devices, one 96%
full, which moved the full-checkout comparison by a factor of about 1.3 in
Postgres's disfavour.

## What this does not measure

Correctness. These harnesses compare latency and throughput; the saga's
compensating path (`ReleaseExpiredReservations`) and the atomic contract's
crash behaviour are exercised by neither.

Contention on the same product. Every client here holds a disjoint triple. When
clients share a product both models contend on it, so that case does not
separate them, but it is not covered.

Multiple tenants. Both contracts partition on `tenant_id`, so a multi-tenant
deployment gets separate conflict domains regardless of which model is used.
