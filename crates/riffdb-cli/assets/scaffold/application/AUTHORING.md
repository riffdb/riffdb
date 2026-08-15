# RiffDB authoring patterns

Keep this page beside the application while replacing the sample domain. It
collects the rules most often needed together; the installed handbook remains
the complete reference.

## Design transaction routes before writing entities

RiffDB proves safety from the model; it cannot infer a transaction boundary
after independently owned entities have been declared. Before writing contract
syntax, make this short worksheet:

1. List every command and the complete set of records it must change atomically.
2. Give each mutation set exactly one aggregate root and one route parameter.
3. List every page or dashboard and the exact route parameter it will receive.
4. Only then define entity keys and indexes, with the route first.

For example, if `ReserveInventory` must atomically change both a purchase order
and stock, `PurchaseOrder` and `Inventory` cannot be independent roots. A safe,
simple store-routed model is:

```riff
enum OrderStatus { Open, Reserved, Fulfilled, Cancelled }

entity Store {
  key (store_id: uuid)
  field name: string<64>
}

entity PurchaseOrder {
  key (store_id: uuid, order_id: uuid)
  field status: OrderStatus
  index by_status (store_id, status, order_id)
}

entity Inventory {
  key (store_id: uuid, product_id: uuid)
  field available: i64
  invariant available_non_negative: available >= 0
  index by_store (store_id, product_id)
}

entity OrderLine {
  key (store_id: uuid, order_id: uuid, product_id: uuid)
  field quantity: i64
  index by_order (store_id, order_id, product_id)
}

aggregate StoreRoot {
  root Store
  child PurchaseOrder
  child Inventory
  child OrderLine
  partition_by store_id
  conflict_key (store_id)
}
```

Now `ReserveInventory` may mutate the order and inventory in one safe command,
and `InventoryDashboard($store_id)` has an exact route and bounded leading
index. The tradeoff is intentionally visible: this first model serializes
writes within one store. Refine ownership later only when every atomic mutation
set still has one root. If records must remain independently rooted, use
separate idempotent commands and treat the operation as a workflow rather than
claiming one atomic transaction.

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

## Encode the selected routed aggregate

Put the tenant, store, or organization route first in every entity key in that
partition. An aggregate has one root; every child key begins with the complete
root key. A command may read another aggregate in the same partition, but every
`create` and `mutate` binding in one command must belong to the single mutation
aggregate selected in the worksheet.

`RDB-C017` means either that two bindings derive different route values or that
the command writes two aggregate roots. Supplying the same route fixes only the
first case. For the second, move the written entities under one business root
or split the operation into separate idempotent commands. RiffDB never turns
cross-aggregate writes into an implicit distributed transaction.
Its two corrective-action codes are `supply_partition_route` and
`model_one_mutation_aggregate`; satisfy both claims before retrying.

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

The relationship source fields must reuse the same key input expressions as
that exact read. For example:

```riff
read Author(site_id, author_id) as author
  else AuthorMissing { author_id: author_id }
create Post(site_id, post_id) as post
  else PostExists { post_id: post_id }
set post.author_id = author_id
```

In grammar v1, `set post.author_id = author.author_id` does not preserve that
structural proof even though the two values are equal after the read. Use the
same `author_id` expression in both the target key and relationship source.
`RDB-C024` and `prove_relationship_target` mean to check all three facts:

1. the complete target-key read dominates the `create` or `mutate` binding;
2. the read declares the missing-target outcome; and
3. every stored relationship component reuses the corresponding target-key
   input expression.

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

## Construct exact values without wire encoding

TypeScript application code must not use JavaScript floating-point numbers or
hand-build two's-complement coefficient bytes. The bundled application runtime
owns exact decimal parsing and generated-schema validation:

```ts
import { exactDecimal, exactMoney } from "@riffdb/application";

const unitPrice = exactMoney("USD", "25.00"); // money<USD>
const taxRate = exactDecimal("0.0825", 8, 4); // decimal<8,4>
```

Pass those values directly to generated command methods. The generated client
owns the tagged CLI representation and rejects currency, precision, or scale
drift. Query results return the same exact shape with `precision`, `scale`, and
`coefficientTwosComplement`; application code never decodes transport JSON.

## Model singular routes as primary-key entities

`one` and `maybe` are point reads. Their predicates must constrain the target
entity's complete primary key; adding a secondary index does not turn a
singular binding into an index lookup. This keeps cardinality and work
independent of data distribution.

When a page uses an external route such as a slug, email address, or vendor
reference that is not the entity's primary key, model the route explicitly:

```riff
entity PostSlug {
    key (site_id: uuid, slug: string<96>)
    field post_id: uuid
}
```

Resolve the route and then the entity with two singular bindings in the same
named query:

```riffql
one route from PostSlug
    where site_id == $site_id && slug == $slug
    else NotFound

one post from Post
    where site_id == $site_id && post_id == route.post_id
    else IntegrityFailure
```

Create or replace the route through a symbolic command. Do not fall back to a
scan or a client-side lookup. Use a bounded `many` binding when the intended
result is genuinely a collection selected through a declared secondary index.

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

## Replace the sample seed plan

Use one JSONL file per symbolic command and list the files in dependency order
under `seed_inputs` in `riffdb.application.json`:

```json
"seed_inputs": [
  "riffdb/seed/01-CreateStore.jsonl",
  "riffdb/seed/02-CreateCustomer.jsonl",
  "riffdb/seed/03-CreateProduct.jsonl"
]
```

The command name is the filename after its numeric prefix. Replace or remove
the sample `CreateItem` file when replacing the sample domain. Successful
application checks print the configured seed count and the exact next command:
use `riffdb dev --seed --run` when the list is nonempty, or `riffdb dev --run`
for an intentionally seedless application. Requesting `--seed` with an empty
list fails before application startup and names any unreferenced JSONL files.

## Use generated read-after-write helpers

Rust generated clients expose query options from the generated module and a
direct helper for the common causal read:

```text
let page = app.order_detail_after_commit(params, command.commit_sequence).await?;
```

TypeScript query methods accept `{ readAfterCommit: command.commitSequence }`.
The generated facade owns parameter serialization, exact operation identity,
result decoding, and cursor handling.
