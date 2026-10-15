---
name: riffdb
description: Safely author and operate the RiffDB project in this repository.
---

# RiffDB project agent guide

## Safety rails

- Treat riffdb.toml and symbolic RiffDB source as the author-owned database interface.
- Use generated application operations; never hand-author protocol identities, storage access, or transaction machinery.
- Resolve uncertain mutations with the same idempotency key before taking another action.
- Use MCP discovery and riffdb://application/guide for the deployed, authorization-filtered application surface.

## Working loop

1. Edit the symbolic schema and bounded named queries.
2. Run riffdb push and review an exact proposed lock identity when one is reported.
3. Run riffdb generate to materialize the selected typed SDKs from that exact lock.
4. Call only generated operations and use explicit freshness or staleness surfaces.
5. Run riffdb status or riffdb diff to compare local and installed identities.

## Commands

```text
riffdb init <application> --generator <language>
riffdb agent init
riffdb push
riffdb generate
riffdb status
riffdb diff
```

## Contract syntax (`.riff`)

```riff
contract SafeApp version 1 {
  entity Item {
    key (tenant_id: uuid, item_id: uuid)
    field title: string<120>
    field created_at: timestamp
    index by_tenant (tenant_id, item_id)
  }
  aggregate ItemRoot {
    root Item
    partition_by tenant_id
    conflict_key (tenant_id, item_id)
  }
  command CreateItem {
    input idempotency_key: string<128>
    input tenant_id: uuid
    input item_id: uuid
    input title: string<120>
    idempotency_key idempotency_key
    create Item(tenant_id, item_id) as item
      else ItemExists { item_id: item_id }
    set item.title = title
    set item.created_at = tx.time
    return Created { item: item }
  }
}
```

## Bounded query syntax (`.riffq`)

```riffq
query ItemPage(
  $tenant_id: Item.tenant_id,
  $after: Cursor?,
  $limit: Limit<100> = 25,
) {
  many items from Item
    where tenant_id == $tenant_id
    order by item_id asc
    after $after
    take $limit
  return Found { items: items { item_id title created_at } }
  outcomes Found
}
```

## Role grants (`riffdb.application.json`)

Every compiled command and named query that an application or MCP client calls MUST be listed in `roles[].commands` or `roles[].queries` inside `riffdb.application.json` (`RDB-AR011` reports any unreferenced operation).

## Safe-by-default generated SDKs

- TypeScript: `const client = SafeAppClient.connect(transport); await client.createItem({ tenantId, itemId, title }); for await (const page of client.itemPagePages({ tenantId })) { ... }`
- Python: `client = SafeAppClient.connect(endpoint, credential_token=token); client.create_item(tenant_id=..., item_id=..., title=...); list(client.item_page_pages(tenant_id=...))`
- Rust: `let mut client = SafeAppClient::connect(&config).await?; client.create_item(&mut session, &input).await?;`
- Go: `client, err := ConnectSafeApp(ctx, config); res, err := client.CreateItem(ctx, session, input)`
- Safety owned by RiffDB by default: omitted `idempotencyKey` / `idempotency_key` is auto-generated and held stable across retries; `read_after_commit` causal watermarks are tracked automatically on every client handle; pagination iterators enforce a bounded `max_pages` ceiling (`64` by default, capped at `1024`).

The POC requires an already running RiffDB service and an ordinary authorized capability credential supplied through protected environment or configuration. Agent initialization never creates or embeds a credential.
