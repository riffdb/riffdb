// req: END-003, END-007, END-008
import { readFile } from "node:fs/promises";
import { DriverApplicationTransport, DriverGeneratedApplicationTransport,
  type DriverApplicationIdentity } from "@riffdb/application";
import { TicketDeskLifecycleClient, TicketDeskLifecycleReactiveClient } from "./client.js";

function requireResult(condition: boolean): asserts condition {
  if (!condition) throw new Error("lifecycle smoke assertion");
}

async function main(): Promise<void> {
  const root = process.argv[2];
  requireResult(root !== undefined && root.length <= 4096);
  const text = await readFile(`${root}/application.identity.json`, "utf8");
  requireResult(text.length <= 4096);
  const wire = JSON.parse(text) as Omit<DriverApplicationIdentity, "contractVersion"> & { contractVersion: number };
  const identity: DriverApplicationIdentity = { ...wire, contractVersion: BigInt(wire.contractVersion) };
  const driver = await DriverApplicationTransport.connect({ socketPath: `${root}/application.sock`, identity });
  try {
    const transport = new DriverGeneratedApplicationTransport(driver);
    const client = new TicketDeskLifecycleClient(transport, 1);
    const organization_id = "01900000-0000-7000-8001-000000000000";
    const ticket_id = "01900000-0000-7000-8004-000000000000";
    const row = await client.getTicket({ organization_id, ticket_id });
    requireResult(row.value.outcome === "Found" && row.value.ticket.ticket_id === ticket_id
      && row.value.ticket.title === "Lifecycle ticket 0");
    const missing = await client.getTicket({ organization_id: "01900000-0000-7000-8001-000000000001", ticket_id });
    requireResult(missing.value.outcome === "NotFound");
    const input = { organization_id, ticket_id, expected_revision: 18446744073709551615n,
      status: "Open", idempotency_key: `ticketdesk-lifecycle-typescript-stale-${identity.contractVersion}` };
    const first = await client.updateTicket(input);
    const replay = await client.updateTicket(input);
    requireResult(first.outcome.outcome === "Stale" && replay.outcome.outcome === "Stale"
      && !first.replayed && replay.replayed && first.commitSequence === replay.commitSequence);
    const after = await client.getTicket({ organization_id, ticket_id });
    requireResult(after.value.outcome === "Found"
      && after.value.ticket.revision === row.value.ticket.revision
      && after.value.ticket.status === row.value.ticket.status);
    const reactive = new TicketDeskLifecycleReactiveClient(transport);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 10000);
    try {
      let seen = false;
      for await (const update of reactive.watchTicketQueueWatch({ organization_id, project_id: "01900000-0000-7000-8003-000000000000" }, undefined, controller.signal)) {
        requireResult(update.type === "snapshot" && update.value.outcome === "Found"
          && update.value.tickets.length > 0 && update.value.tickets.length <= 50);
        seen = true;
        break;
      }
      requireResult(seen);
    } finally {
      clearTimeout(timer);
      controller.abort();
    }
    console.log(JSON.stringify({ schema: "riffdb.ticketdesk-lifecycle-typescript-smoke/v1",
      named_read: true, partition_isolation: true, caller_fence: true,
      idempotency: true, state_unchanged: true, live_query_snapshot: true }));
  } finally {
    await driver.shutdown();
  }
}

main().catch(() => {
  console.error("ticketdesk-lifecycle-typescript-smoke: failed");
  process.exitCode = 1;
});
