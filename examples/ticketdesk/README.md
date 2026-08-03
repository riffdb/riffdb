# TicketDesk reactive acceptance application

TicketDesk is the long-lived acceptance application for symbolic commands,
named RiffQL, partitioned events, durable consumers, live queries, contextual
agent work, generated clients, and the application-owned browser relay.

The canonical author source is `riffdb.application.json`. It compiles to
Application Lock V5 and pins the contract, 13 named queries, `TicketActivity`
reactive module, application and agent roles, and Rust, TypeScript, Python, and
MCP output. `TicketCreated` is partitioned by `organization_id`; the generated
surface exposes `TicketEvents`, `TicketQueueWatch`, `TicketPageWatch`, and
`TriageTicket`.

The three development roles are intentionally disjoint. `TicketDeskSeeder`
can create prerequisite organizations, users, and projects but cannot read or
watch application state. `TicketDeskApplication` owns browser reads and the
two browser-facing commands. `TicketDeskAgent` owns event/context consumption
and only the declared `CreateComment` reaction.

Application source contains no numeric schema IDs, protobuf construction,
encoded-key parsing, field masks, `GetEntity`, `ScanIndex`, or public N+1
loops. Browsers connect to the application server's authenticated SSE relay;
the RiffDB endpoint and capability remain server-side.

Review and regenerate the exact application from the repository root:

```bash
cargo run -p riffdb-cli --bin riffdb -- application lock --check \
  examples/ticketdesk/riffdb.application.json \
  --lock riffdb.application.lock.json
./scripts/check-application-bindings
```

Deploy to a configured development database with an operator credential:

```bash
riffdb --config ~/.config/riffdb/client.toml \
  --credential-file ~/.config/riffdb/operator.credential \
  application deploy examples/ticketdesk/riffdb.application.json \
  --lock riffdb.application.lock.json \
  --provision-role TicketDeskApplication \
  --credential-output ~/.config/riffdb/ticketdesk-application.credential
```

Build and start the application-owned browser relay:

```bash
npm --prefix examples/ticketdesk/web install
npm --prefix examples/ticketdesk/web run build
cd examples/ticketdesk/web
node dist/server.js http://127.0.0.1:7432 \
  ~/.config/riffdb/ticketdesk-application.credential \
  ~/.local/bin/riffdb
```

The server prints `ticketdesk-reactive-web-ready-v1` and its loopback port.

Run the machine boundary:

```bash
./scripts/check-ticketdesk-symbolic-boundary
```

The Rust application transport remains public gRPC over a reused HTTP/2
connection. RiffQL removes structural RPC multiplication: the server plans
batch reads and joins and executes a complete page against one snapshot.
Contextual work hydrates `TicketPage` in one shared snapshot and reacts only
through generated, causally fenced, idempotent command helpers.
