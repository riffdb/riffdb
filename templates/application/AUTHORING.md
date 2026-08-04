# RiffDB authoring patterns

Keep this page beside the application while replacing the sample domain. It
collects the rules most often needed together; the installed handbook remains
the complete reference.

## Iterate without manufactured lock failures

While editing author-owned contract, query, role, or seed files, run:

```text
riffdb application check --source-only
```

This compiles the complete symbolic application and its least-authority roles,
but deliberately does not compare the old compiler-owned lock or generated
files. At a review point, accept the new identities once and verify them:

```text
riffdb application lock --write
riffdb application check
```

Exact `application check` must fail when source and lock differ. Do not edit
`riffdb.application.lock.json` or `generated/` by hand.

## Choose unambiguous symbolic names

Grammar words cannot be identifiers. In particular, use `origin` or
`source_label` instead of `source`, and use `PurchaseOrder` with a binding such
as `purchase` instead of an `order` binding. The complete reserved-word list is
in the installed contract authoring reference. Names are scoped: enum variants
belong to their enum, outcomes to their command, and indexes to their entity.
When a name is genuinely duplicated, the diagnostic identifies both source
spans. Contract and query source support `//` line comments, not block comments.

## Model one routed aggregate

Put the tenant or organization route first in every entity key that belongs to
that partition. An aggregate has one root; child keys begin with the complete
root key. A command may read other entities in the same partition, but every
`create` and `mutate` binding in one command belongs to one aggregate.

```riff
entity PurchaseOrder {
  key (organization_id: uuid, order_id: uuid)
  field status: OrderStatus
  index by_status (organization_id, status, order_id)
}

entity OrderLine {
  key (organization_id: uuid, order_id: uuid, product_id: uuid)
  field quantity: i64
  index by_order (organization_id, order_id, product_id)
}

aggregate OrderRoot {
  root PurchaseOrder
  child OrderLine
  partition_by organization_id
  conflict_key (organization_id, order_id)
}
```

If one atomic operation truly changes two independently keyed roots, remodel
them as one business aggregate or use two idempotent commands. RiffDB does not
turn cross-aggregate writes into an implicit distributed transaction.

## Prove relationships inside commands

Before creating or changing fields that form a declared relationship, bind the
complete target key and declare its missing-target outcome. Put this exact read
before the mutable binding:

```riff
read Product(organization_id, product_id) as product
  else ProductMissing { product_id: product_id }
create OrderLine(organization_id, order_id, product_id) as line
  else LineExists { product_id: product_id }
```

A separate query followed by a command is not a relationship proof and would
reintroduce a check-then-write race.

## Preserve numeric invariants transaction-current

Read and mutate the entity in the same compiled command, state the precondition
with `require`, and retain the entity invariant:

```riff
require EnoughStock: inventory.available >= quantity
  else InsufficientStock { product_id: product_id }
set inventory.available = inventory.available - quantity
```

```riff
invariant available_non_negative: available >= 0
```

Use one caller-stable idempotency input for the command. Reuse it after an
uncertain response; generated clients recover the original outcome.

## Make every page indexed, ordered, and bounded

For `many` reads, predicates must supply the leading index fields and `order
by` must match the remaining index key in forward or reverse order. Use fixed
`take` values first; a `Limit` parameter is charged at the full 500-row maximum.

For junction-to-entity or line-to-product hydration, the first collection must
be a bounded, ascending, duplicate-free set of complete target key components.
The second collection uses exactly one `in` field, has an equal-or-smaller
bound, orders by that same field ascending, and declares a missing-target
outcome:

```riffql
many lines from OrderLine
    where organization_id == $organization_id
      && order_id == purchase.order_id
    order by product_id asc
    take 50

many products from Product
    where organization_id == $organization_id
      && product_id in lines.product_id
    order by product_id asc
    take 50
    else IntegrityFailure
```

If the first collection can contain the same `product_id` more than once, it
is not a key set. Model one line per product, copy the small immutable display
field onto the line, or declare a projection; RiffDB will not silently
deduplicate it.

## Use generated read-after-write helpers

Rust generated clients expose query options from the generated module and a
direct helper for the common causal read:

```text
let page = app.order_detail_after_commit(params, command.commit_sequence).await?;
```

TypeScript query methods accept `{ readAfterCommit: command.commitSequence }`.
The generated facade owns parameter serialization, exact operation identity,
result decoding, and cursor handling.
