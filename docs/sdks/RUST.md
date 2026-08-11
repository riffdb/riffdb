# Rust Applications

Rust applications use the public `riffdb-client-rust` facade plus a generated
module for the exact application lock. They do not depend on server, storage,
policy, compiler, Tonic, Prost, or raw Protobuf crates.

For a remote daemon, construct the closed `riffdb_config::TlsClientConfig` and
call `RiffDbClient::connect_verified_tls`. The client loads one protected CA
root, verifies the exact DNS/IP identity, and has no native-root, trust-all,
cleartext-fallback, client-certificate, cipher-suite, or provider selector.
Long-lived hosts can retain `VerifiedTlsConnector`; it atomically adopts only a
complete valid trust-root replacement for new connections and retains the last
valid snapshot otherwise.
See [Remote and Local Application Ingress](../operations/REMOTE-INGRESS.md).

`StableApplicationClient::connect_verified_tls` constructs the
application-only facade directly from the same closed TLS configuration. The
first-party [Driver Host](DRIVER-HOST.md) uses that path to centralize trust,
pooling, retry, cancellation, uncertainty, and reactive resumption for target
languages without exposing the kernel client.

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

Operational applications can preflight the exact authorized server feature
registry with `StableApplicationClient::preflight_application_features` before
constructing the generated facade. An unavailable feature never authorizes a
client-side scan or filter. See [Generated Operational
Queries](../getting-started/OPERATIONAL-QUERIES.md).

Generated entity and outcome decoders consume every wire field exactly once.
An `optional<T>` field maps a wire null to `None` and a present value to
`Some(T)` through the same typed decoder used for required fields. This applies
to command outcomes as well as query results; application code never probes or
removes a field by its compiler ID.

Application Source V4 also generates typed durable event consumers and live
named queries. Event deliveries retain exact lease evidence for generated
acknowledge and negative-acknowledge methods. Live updates use a closed union;
persist each applied cursor and clear retained results on `Terminal`. See
[Reactive Application Clients](../reactive/CLIENTS.md).

Generated command batch methods accept `GeneratedBatchOptions` concurrency
from 1 through 384 and at most 4,096 inputs. The setting bounds independent
in-flight items; each public `ExecuteBatch` request still carries at most 16
ordinary commands, with separate identities, outcomes, and recovery. At the
maximum, Rust opens at most 24 bounded transport exchanges; it does not create
one 384-command RPC or application transaction.

## Retry rule

Use one caller-owned idempotency key for one logical command and retain it until
the outcome is known. The stable client may retry only according to its bounded
attempt policy. `OutcomeUnknown` means the server result could not be resolved;
it does not mean the command failed.

## Migration administration

Contract migration is a kernel administration surface, not generated
application API. The stable facade exposes `CheckContractMigration`,
`ApplyContractMigration`, `generate_contract_migration_operation_id`, and the
three migration client methods. Construct one immutable submission and reuse it
across bounded retries; only the outer request ID changes.

```rust,ignore
let check_id = generate_contract_migration_operation_id()?;
let check = CheckContractMigration::new(
    check_id,
    candidate_bundle.clone(),
    migration_bundle.clone(),
)?;
let checked = client
    .check_contract_migration_with_retry(&check, attempts, &metadata)
    .await?;

let apply_id = generate_contract_migration_operation_id()?;
let apply = ApplyContractMigration::new(
    apply_id,
    candidate_bundle,
    migration_bundle,
    reviewed_migration_hash,
)?;
let accepted = client
    .apply_contract_migration_with_retry(&apply, attempts, &metadata)
    .await?;
```

Check and apply require different operation IDs because their canonical input
identities differ. After an uncertain apply, retain the apply ID and call
`get_contract_migration_operation` after the selected database reopens. The
current TypeScript and Python facades intentionally do not expose this
administrative surface.

## Public API

The built handbook publishes Rustdoc only for `riffdb-client-rust`. See the
[Rust API reference](../reference/RUST-API.md). Internal workspace crates are
implementation boundaries, not supported application APIs.
