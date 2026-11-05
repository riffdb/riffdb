# PostgreSQL Safety Comparison

RiffDB includes equivalent LegalSpend and TicketDesk workloads for PostgreSQL
and RiffDB. The comparison is a semantic demonstration first and a benchmark
second: it identifies mistakes an ordinary PostgreSQL application can make and
the corresponding write path RiffDB does not expose.

## Run the public comparison

The PostgreSQL side requires a disposable database URL. Never point the runner
at a database containing valuable data.

```bash
export RIFFDB_COMPARISON_POSTGRES_URL='postgres://localhost/riffdb_comparison'
cargo test --locked --manifest-path examples/budget-comparison/Cargo.toml \
  --test public_comparison -- --nocapture
```

The TicketDesk performance runner is under `examples/app-baseline`. Its README
records the exact setup, workload, and measurement rules. Benchmark numbers are
environment-specific; the checked semantic assertions are the durable result.
`benchmarks/run-app-baseline-python`,
`benchmarks/run-app-baseline-typescript`, and
`benchmarks/run-app-baseline-golang` run that same mix from Python, TypeScript,
or Go so the language-runtime cost is visible: PostgreSQL still implements
the `postgres_safe_app` obligations in that language's SQL, while the RiffDB
path uses the generated TicketDesk client with no application-language safety
code (`riffdbd` enforces authorization, idempotency, audit, events, and
outbox). TypeScript and Go reach `riffdbd` through `riffdb-driverd`. These
runners are not release evidence.

## Patterns under test

| Application mistake | PostgreSQL responsibility | RiffDB boundary |
|---|---|---|
| Read a balance, check it, then update later | Correct isolation, locking, constraint, and retry loop | Compiled command records the influential read and revalidates it at commit |
| Retry after an uncertain response with a new operation identity | Application idempotency table and atomic outcome handling | Caller key plus canonical input hash resolves one durable typed outcome |
| Update state and enqueue an external effect separately | Transactional outbox discipline | Effect intent is part of the command's atomic commit graph |
| Grant an agent generic write access | SQL permission and query-generation controls | Policy exposes only compiled named commands and bounded named queries |

PostgreSQL can implement each safe pattern. The negative controls intentionally
show what happens when the application does not. RiffDB's value is that its
normal application protocol cannot express the unsafe alternative.

## What the comparison does not claim

- It is not a general database performance ranking.
- It does not compare analytical SQL or ad-hoc joins; RiffDB does not provide
  them.
- It does not claim agents can skip contract review. Unsafe semantics in an
  accepted contract remain unsafe semantics.
- It does not place PostgreSQL on RiffDB's critical path. RiffDB owns its
  standalone storage and commit semantics.

Review the source in `examples/budget-comparison` and `examples/app-baseline`
before interpreting measurements.

## Brownfield Incremental Adoption: Running RiffDB Beside an Existing PostgreSQL SaaS

If you operate an existing PostgreSQL application, you should **not** rewrite
your system of record or introduce dual writes across two databases. Instead,
carve out **one self-contained operational workflow** where concurrency,
idempotent retries, leased background work, or least-privilege agent execution
already hurts, and let RiffDB own that workflow's facts end-to-end.

### 1. Workload-Fit Guide: What Stays in PostgreSQL vs. What Moves to RiffDB

| Dimension | Keep in PostgreSQL | Move as an Independent Workflow to RiffDB |
|---|---|---|
| **Primary workload** | Users, organizations, billing plans, settings, broad CRUD, and ad-hoc BI reporting / multi-table SQL joins | High-contention quotas, credit/budget reservations, seat/inventory claims, approval state machines, and agent-executed workflows |
| **Identity boundary** | Owns user authentication, sessions, and primary `tenant_id` / `user_id` UUIDs | Accepts your application's existing `tenant_id` and `user_id` UUIDs as typed command inputs and principal facts (no RiffDB userland auth required) |
| **Write path** | Application-assembled SQL (`INSERT`/`UPDATE`) across arbitrary tables | Compiled, idempotent commands (`ReserveQuota`, `ReleaseQuota`, `ApproveRequest`) with closed outcomes and atomic events/outbox |
| **Cross-system sync** | Reads RiffDB outcomes synchronously from the command return value, or consumes RiffDB outbox/changelog events asynchronously | Never performs a distributed two-phase commit with PostgreSQL; emits durable outbox webhooks (`[maintenance.outbox]` `http_webhook`) or `riffdb changelog export` |

### 2. Worked Pilot: Moving API Quota & Credit Reservations to RiffDB Without Dual Writes

Suppose your PostgreSQL SaaS stores `organizations (id, plan_tier)` and `users
(id, org_id)`, and currently suffers from lock contention or double-deductions
on `UPDATE api_quotas SET remaining = remaining - $1 WHERE org_id = $2`.

To move quota reservations to RiffDB without dual writes:

1. **PostgreSQL keeps `organizations` and `users`.** Drop the mutable
   `remaining` counter from the PostgreSQL hot path; RiffDB is now the single
   authoritative owner of `QuotaEnvelope`.
2. **Initialize RiffDB inside your existing repository** (`riffdb init
   quota-service --generator typescript` or your backend language) and key
   `QuotaEnvelope` directly by your PostgreSQL `org_id` and `feature_key`:

```riff
contract QuotaPilot version 1 {
  entity QuotaEnvelope {
    key (org_id: uuid, feature_key: string<64>)
    field remaining: u64
    field limit_per_period: u64
    field updated_by_user_id: uuid
    index by_org (org_id, feature_key)
  }

  aggregate OrgQuotas {
    root QuotaEnvelope
    partition_by org_id
    conflict_key (org_id, feature_key)
  }

  event QuotaDepleted {
    org_id: uuid
    feature_key: string<64>
    triggered_by_user_id: uuid
  }

  command ProvisionQuota {
    input request_id: string<128>
    input org_id: uuid
    input feature_key: string<64>
    input limit_per_period: u64
    input actor_user_id: uuid
    idempotency_key request_id

    init_or_mutate QuotaEnvelope(org_id, feature_key) as q default {
      remaining = limit_per_period
      limit_per_period = limit_per_period
      updated_by_user_id = actor_user_id
    }
    overwrite q.limit_per_period = limit_per_period
    overwrite q.remaining = limit_per_period
    overwrite q.updated_by_user_id = actor_user_id

    return Provisioned { quota: q }
  }

  command ConsumeQuota {
    input request_id: string<128>
    input org_id: uuid
    input feature_key: string<64>
    input units: u64
    input actor_user_id: uuid
    idempotency_key request_id

    mutate QuotaEnvelope(org_id, feature_key) as q else QuotaMissing {}
    require HasCapacity: q.remaining >= units else QuotaExhausted {
      remaining: q.remaining
    }

    set q.remaining = q.remaining - units checked_by HasCapacity
    overwrite q.updated_by_user_id = actor_user_id

    return Consumed { remaining: q.remaining }
  }
}
```

3. **Failure recovery without dual writes:**
   - When a customer upgrades their plan in PostgreSQL, your billing webhook or
     worker calls `ProvisionQuota` with a deterministic `request_id =
     "upgrade:<org_id>:<plan_version>"`. If the call times out and retries,
     `ProvisionQuota` is idempotent.
   - On every API request, your service calls `ConsumeQuota` using the request's
     idempotency key. Neither database writes to the other during the request.
4. **Exporting back to PostgreSQL or a warehouse (Pillar exit path):**
   - If your analytics warehouse or billing dashboard in PostgreSQL needs a copy
     of RiffDB state or history, use `riffdb changelog export` (see
     [Upgrade and Removal](../upgrade-removal.md)) or configure
     `[maintenance.outbox]` `http_webhook` delivery in `riffdbd.toml` to push
     committed outbox records to your service. You never lose data portability.

### 3. SQL-to-RiffDB Translation & Route Selection Exercise (`partition_by` vs. `conflict_key`)

The most common modeling mistake when coming from PostgreSQL is confusing
`partition_by` (the visibility and multi-entity transaction boundary) with
`conflict_key` (the row-level lock/serialization boundary):

| Representative SQL Operation | RiffDB Equivalent | Route Selection (`partition_by` vs. `conflict_key`) |
|---|---|---|
| `SELECT * FROM tasks WHERE org_id = $1 ORDER BY created_at DESC LIMIT 20` | Named RiffQL query (`many tasks from Task where org_id == $org_id order by created_at desc, task_id asc take 20`) backed by `index by_created (org_id, created_at, task_id)` | **`partition_by org_id`**: A bounded index scan always runs within one partition. If you mistakenly declared `partition_by task_id`, each task would live in its own partition and you could never list tasks for an `org_id`. |
| `BEGIN; SELECT remaining FROM quotas WHERE org_id = $1 AND feature = $2 FOR UPDATE; UPDATE quotas SET remaining = remaining - $3 ...; COMMIT;` | Compiled command `ConsumeQuota` with `mutate QuotaEnvelope(org_id, feature_key)` and `require HasCapacity: q.remaining >= units` | **`partition_by org_id`, `conflict_key (org_id, feature_key)`**: Widening the partition to `org_id` lets org-wide queries see all features, while narrowing `conflict_key` to `(org_id, feature_key)` means concurrent updates to *different* features in the same org never block each other. |
| `BEGIN; UPDATE inventory SET stock = stock - 1 WHERE org_id = $1 AND sku = $2; INSERT INTO orders (org_id, order_id, sku) VALUES ($1, $3, $2); COMMIT;` | One cross-aggregate command that mutates `InventoryItem(org_id, sku)` and creates `Order(org_id, order_id)` in a single atomic commit | Both aggregates declare **`partition_by org_id`** so one command can write both atomically, while keeping independent conflict keys `(org_id, sku)` and `(org_id, order_id)`. |

See [Choosing a Partition Route](../contracts/AUTHORING.md#choosing-a-partition-route)
for the complete authoring rules.
