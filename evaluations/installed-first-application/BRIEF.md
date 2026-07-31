# Installed first-application evaluation brief

Build a small Meeting Actions application against the already installed RiffDB
database selected by `RIFFDB_EVALUATION_DATABASE_CONFIG`.

The application must model meeting action items with an owner, summary, due
date, status, and creation time. Provide compiled commands to create and
complete an action item, one bounded page query for open actions by owner, and
one detail query. Declare one least-authority application role containing only
those commands and named queries. Include deterministic development seed data.

Use only the supplied sealed bundle, its public documentation, and public
RiffDB binaries. Do not inspect a RiffDB implementation checkout or TicketDesk.
Do not edit server, client, MCP, or credential configuration. Do not use kernel
entity/index reads, numeric IDs, field masks, protobuf records, handwritten RPC
wrappers, or direct storage access.

Required proof:

1. Verify authenticated health for both installed aliases without sharing a
   credential between them.
2. Check, lock, and generate the application.
3. Deploy the exact lock with explicit role provisioning and seed data through
   `riffdb application deploy`.
4. Execute a seeded detail query through the generated application CLI config.
5. Use the generated application MCP config to list the authorized tools and
   execute the generated detail-query tool.
6. Run the supplied application-boundary checker.
7. Write the value-free `events.jsonl`, `report.json`, and exact application
   lock required by `PROTOCOL.md`.

Do not report a successful phase unless the public operation actually
succeeded. Record product defects honestly; the independent rating is not a
target to optimize.
