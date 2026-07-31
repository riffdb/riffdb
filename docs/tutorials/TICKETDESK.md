# TicketDesk End to End

TicketDesk is the main symbolic application example. It demonstrates scoped
multi-tenant entities, bounded page queries, dependent reads, named commands,
generated clients, role-derived authority, and same-workload PostgreSQL
measurement.

## Run the disposable application

From the RiffDB source checkout:

```bash
cargo run --locked -p riffdb-cli -- dev --seed
```

The command prints the loopback endpoint and scoped credential after the server
is ready. It creates 276 rows by invoking compiled commands; it does not import
storage files or bypass authorization.

To execute the checked application runner as well:

```bash
cargo run --locked -p riffdb-cli -- dev --seed --run
```

## Read one page

TicketDesk's named page query resolves a ticket and its bounded dependent data
in one server request and one authoritative snapshot. The query compiler owns
the index selection, dependent-key proof, cardinality limits, result schema,
and cursor codec.

Inspect the plan without executing it:

```bash
cargo run --locked -p riffdb-cli -- query explain \
  queries/ticketdesk/ticket_page.riffq
```

For an installed generated application, invoke named queries through its
generated Rust, TypeScript, Python, or MCP operation. Do not copy internal IDs
from the explain output into application code.

## Mutate through commands

TicketDesk mutations name commands such as create, assign, comment, or status
transition. Each call carries an idempotency key and returns one declared
outcome. The role used by the application contains exact named commands and
queries; it has no kernel entity or index authority.

## Verify the boundary

```bash
./scripts/check-ticketdesk-symbolic-boundary
./scripts/check-application-bindings
```

These checks reject raw Protobuf construction, storage dependencies, numeric
schema identities, handwritten transport adapters, and generated-artifact
drift in the application path.

## Explore further

- [Build a symbolic application](../getting-started/SYMBOLIC-APPLICATIONS.md)
- [RiffQL planning and bounds](../riffql/PLANNING.md)
- [Safe application profiles](../getting-started/SAFE-APPLICATION-PROFILES.md)
- [TicketDesk acceptance evidence](../getting-started/TICKETDESK-ACCEPTANCE.md)
