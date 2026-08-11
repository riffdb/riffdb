# Generated operational queries

Operational RiffQL gives an application a finite set of compiler-owned read
plans without accepting a predicate tree, field name, operator, order clause,
or index from the caller. A deployed named query may expose typed optional
values, null/existence tests, binary prefix lookup, stable cursors, and bounded
exact aggregates. Every possible optional-value presence combination is
compiled, authorized, costed, and indexed before deployment.

Generate bindings from the reviewed exact lock:

```bash
riffdb application lock --write riffdb.application.json
```

The generated Rust, Go, TypeScript, and Python methods accept only the values
declared by the query. For example, a relation filter is an optional string,
not a generic condition:

```rust,ignore
let page = db.list_fga_tuples(ListFgaTuplesParams {
    store_id,
    relation: Some("viewer".to_owned()),
    after: None,
}).await?;
```

```typescript,ignore
const page = await db.listFgaTuples({
  store_id: storeId,
  relation: "viewer",
});
```

Go and TypeScript use the long-lived Rust driver host; Python and Rust may use
the verified-TLS application transport directly. In every case the result is a
closed generated outcome with bounded arrays, optional values, opaque cursors,
and exact integer/decimal carriage. Aggregate sums that do not assert a wire
precision are decoded under the generated result schema; a conflicting wire
precision or scale is rejected.

## Cursor rule

Persist the opaque `next_cursor` returned with a page and submit it only to the
same exact generated operation, contract, module, parameters, principal, and
page shape. Do not decode it. An invalid, expired, cross-principal, or stale
cursor is a typed failure; clients must not silently restart from the first
page.

## Feature preflight

The authorized application catalog returns the closed
`riffdb.application-catalog/v1` feature registry for one exact contract. Rust
applications can call
`StableApplicationClient::preflight_application_features`. The checked result
contains only the exact contract identity, active module hashes, and the six
closed feature states; it contains no raw catalog symbols, numeric compiler
identities, capability record, IR, or storage key.

`unavailable` is a prohibition, not a request for client emulation. In the
current alpha, binary UTF-8 prefix lookup is available and
`unicode_fold_v1` remains explicitly unavailable. Application code must not
replace an unavailable feature with client filtering, raw reads, N+1 requests,
or handwritten query text.

## CLI and MCP

The same named query can be invoked by CLI or its generated MCP tool. CLI
parameters use the public tagged JSON value format; MCP input and output JSON
Schemas contain only symbolic parameters and bounded result fields. Neither
surface accepts an index, plan, field ID, or arbitrary predicate.

The retained acceptance corpus is:

```bash
./scripts/adapter-operational-query-acceptance --all-languages
```

It deploys one exact application over verified TLS, seeds through compiled
commands, and checks OpenFGA tuple filtering, an MLflow exact metric dashboard,
Payload binary-prefix and null pages, and a Woodpecker state queue through
Rust, Go, TypeScript, Python, CLI, and generated MCP schemas. It also proves a
malformed cursor fails closed and scans the application runners for kernel,
storage, numeric-ID, raw-transaction, and client-filter escape hatches.
