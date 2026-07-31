# Rust Applications

Rust applications use the public `riffdb-client-rust` facade plus a generated
module for the exact application lock. They do not depend on server, storage,
policy, compiler, Tonic, Prost, or raw Protobuf crates.

## Generated boundary

Generate from the reviewed lock:

```bash
riffdb application generate --locked
```

The generated module contains exact contract, query-module, command-plan, and
artifact identities; checked input and result types; declared outcomes; and the
operation-specific client methods. The stable facade owns transport validation,
retry classification, fresh request IDs, and uncertain-outcome recovery.

```rust,ignore
let outcome = client
    .create_item(CreateItemInput {
        item_id,
        title: "Review contract".to_owned(),
    })
    .await?;

match outcome {
    CreateItemOutcome::Created(value) => println!("{}", value.item_id),
    CreateItemOutcome::AlreadyExists(_) => {}
}
```

The exact generated names come from your contract and lock. Treat a changed
generated diff like an API change: review the source change and new lock before
accepting it.

Generated entity and outcome decoders consume every wire field exactly once.
An `optional<T>` field maps a wire null to `None` and a present value to
`Some(T)` through the same typed decoder used for required fields. This applies
to command outcomes as well as query results; application code never probes or
removes a field by its compiler ID.

## Retry rule

Use one caller-owned idempotency key for one logical command and retain it until
the outcome is known. The stable client may retry only according to its bounded
attempt policy. `OutcomeUnknown` means the server result could not be resolved;
it does not mean the command failed.

## Public API

The built handbook publishes Rustdoc only for `riffdb-client-rust`. See the
[Rust API reference](../reference/RUST-API.md). Internal workspace crates are
implementation boundaries, not supported application APIs.
