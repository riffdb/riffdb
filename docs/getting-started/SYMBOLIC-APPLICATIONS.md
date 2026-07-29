# Build a symbolic application in ten minutes

RiffDB applications read with RiffQL and mutate with compiled contract
commands. The supported kernel gRPC protocol remains available for low-level
tools and conformance tests, but application code does not need entity IDs,
field IDs, index IDs, masks, encoded keys, or protobuf record construction.

From the repository root, start the complete disposable TicketDesk environment:

```bash
cargo run -p riffdb-cli -- dev --seed
```

`riffdb dev`:

1. starts `riffdbd` on a random loopback port;
2. bootstraps an operator through the production capability path;
3. deploys the TicketDesk contract and immutable query module;
4. creates a named-only capability from the default
   `ticketdesk-application` role preset;
5. creates 276 rows through compiled commands; and
6. prints the endpoint and protected application credential path.

Nothing in this path bypasses authorization, command semantics, audit, the
commit coordinator, or durable redb storage. The disposable directory and
credentials are removed when the process exits.

Use `--watch` to redeploy changed contract and `.riffq` sources. Query-only
changes increment the query-module version without requiring client
regeneration:

```bash
cargo run -p riffdb-cli -- dev --watch
```

Use `--role ticketdesk-agent` for the separately named agent allowlist. It
still receives no ad-hoc query or kernel authority. Use
`--role ticketdesk-kernel` only for low-level diagnosis; it receives no
application command or RiffQL authority and cannot seed. There is no combined
preset. See
[Safe application profiles](SAFE-APPLICATION-PROFILES.md).

Use a directory of JSON command inputs with bounded concurrency:

```bash
cargo run -p riffdb-cli -- dev \
  --seed-dir ./my-seed \
  --seed-concurrency 4
```

The concurrency bound is `1..8`. Every file is submitted through `riffdb
command run`; seed files are not storage imports.

## Write a read

Save a bounded query as `queries/my_page.riffq`:

```riffql
query MyPage(
    $organization_id: Organization.organization_id,
    $ticket_id: Ticket.ticket_id,
) {
    one ticket from Ticket
        where organization_id == $organization_id
            && ticket_id == $ticket_id
        else NotFound

    return Found {
        ticket: ticket {
            ticket_id
            title
            status
        }
    }

    outcomes Found | NotFound
}
```

Check and explain before execution:

```bash
riffdb query check queries/my_page.riffq
riffdb query explain queries/my_page.riffq
```

Deploy a directory and run by name:

```bash
riffdb query deploy queries/ \
  --module-name my_app \
  --module-version 1 \
  --contract-lineage TicketDesk \
  --contract-version 1

riffdb query run-named MyPage \
  --contract-lineage TicketDesk \
  --contract-version 1 \
  --parameters parameters.json
```

The server resolves symbols, type-checks parameters, derives the complete
authorization request, verifies one partition and bounded access, chooses the
index plan, executes the page in one authoritative snapshot, and returns
name-addressed typed records.

## Write a mutation

Application writes remain compiled commands:

```bash
riffdb command run CreateTicket --input create-ticket.json
```

The input object uses contract field names. Idempotency, declared outcomes,
provenance, events, and the commit record retain their existing atomic
semantics.

## Generated application clients

Stable named operations can generate Rust, TypeScript, and MCP artifacts:

```bash
./scripts/generate-query-clients
./scripts/generate-query-clients --check
```

Generated query requests pin the exact contract lineage, contract version,
contract bundle hash, query-module hash, and query name. The facade owns
parameter encoding, result decoding, typed outcomes, cursors, read fences,
public errors, and command uncertainty recovery. A command retry retains its
original idempotency identity and uses outcome resolution when the commit
result is uncertain.

Rust application code calls only generated names and types:

```rust,no_run
use riffdb_client_rust::{AttemptBudget, CallMetadata, QueryOptions};
use riffdb_ticketdesk::{TicketDeskClient, TicketPageParams};

# async fn example(
#     application: riffdb_client_rust::StableApplicationClient,
#     metadata: CallMetadata,
# ) -> Result<(), riffdb_client_rust::ApplicationClientError> {
let mut db = TicketDeskClient::new(
    application,
    metadata,
    AttemptBudget::new(3).expect("positive configured attempt budget"),
);
let page = db
    .ticket_page_with_options(
        TicketPageParams {
            organization_id: "c93bf186-4901-4cb6-8af2-fec65eb928e6".to_owned(),
            ticket_id: "535bbe7a-3f53-4792-97bb-5d9a692be0ef".to_owned(),
            comments_after: None,
        },
        QueryOptions::new().read_after_commit(1843),
    )
    .await?;
# let _ = page;
# Ok(())
# }
```

TypeScript exposes the same named operations and observations, including
`bigint` application heads and commit sequences. MCP query and command schemas
are generated from the same registry and carry the same exact identities.

Code generation is optional for exploration: ad-hoc RiffQL and named CLI/MCP
execution remain first-class. It is the normal boundary for stable application
code. See [Application manifest](APPLICATION-MANIFEST.md).

See [RiffQL language](../riffql/LANGUAGE.md),
[planning](../riffql/PLANNING.md), and [query modules](../riffql/MODULES.md).
