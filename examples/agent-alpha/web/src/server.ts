import { createServer } from "node:http";

import { CliApplicationTransport } from "@riffdb/application";

import {
  AgentAlphaClient,
  type CreateItemOutcome,
  type ItemPageResult,
} from "./generated/client.js";

const ITEM_ID = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b11";

const [endpoint, credentialFile, riffdbPath] = process.argv.slice(2);
if (endpoint === undefined || credentialFile === undefined || riffdbPath === undefined) {
  throw new Error("usage: server ENDPOINT CREDENTIAL_FILE RIFFDB_PATH");
}

const transport = new CliApplicationTransport({ riffdbPath, endpoint, credentialFile });
const application = new AgentAlphaClient(transport, 3);

const server = createServer(async (request, response) => {
  try {
    if (request.method !== "GET" || request.url !== "/item") {
      response.writeHead(404).end();
      return;
    }
    const command = await application.createItem({
      idempotency_key: "agent-alpha-typescript-web-item",
      item_id: ITEM_ID,
      title: "TypeScript generated web application",
    });
    assertCommandOutcome(command.outcome);
    const page = await application.itemPage(
      { item_id: ITEM_ID },
      command.commitSequence === undefined ? {} : { readAfterCommit: command.commitSequence },
    );
    assertPage(page.value);
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      outcome: command.outcome.outcome,
      title: page.value.item.title,
      applicationHead: page.applicationHead.toString(),
      readAfterCommit: command.commitSequence?.toString() ?? null,
    }));
  } catch {
    response.writeHead(500, { "content-type": "application/json" });
    response.end('{"error":"application request failed"}');
  }
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("invalid web address");
  process.stdout.write(`agent-alpha-web-ready-v1\t${address.port}\n`);
});

function assertCommandOutcome(outcome: CreateItemOutcome): void {
  if (outcome.outcome !== "Created" && outcome.outcome !== "ItemExists") {
    throw new Error("unexpected generated command outcome");
  }
}

function assertPage(
  page: ItemPageResult,
): asserts page is Extract<ItemPageResult, { outcome: "Found" }> {
  if (page.outcome !== "Found") throw new Error("generated page did not find the item");
}
