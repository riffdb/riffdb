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

Use `--role ticketdesk-agent` only when the process needs explicit ad-hoc
RiffQL check/explain/execute authority. Use `--role ticketdesk-kernel` only for
low-level diagnosis; it receives no application command or RiffQL authority and
cannot seed. There is no combined preset. See
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

## Optional generated clients

Stable named operations can generate Rust, TypeScript, and MCP artifacts:

```bash
./scripts/generate-query-clients
./scripts/generate-query-clients --check
```

Generated query requests pin the exact contract lineage, contract version,
contract bundle hash, query-module hash, and query name. They include a
response-identity verifier. Code generation is optional; ad-hoc RiffQL and
named CLI/MCP execution remain first-class.

See [RiffQL language](../riffql/LANGUAGE.md),
[planning](../riffql/PLANNING.md), and [query modules](../riffql/MODULES.md).
