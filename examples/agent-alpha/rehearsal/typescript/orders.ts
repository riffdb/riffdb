import { createServer } from "node:http";

import { CliApplicationTransport } from "@riffdb/application";

import {
  AgentOrdersClient,
  CONTRACT_BUNDLE_HASH,
  CONTRACT_LINEAGE,
  CONTRACT_VERSION,
  CREATE_STORE_PLAN_HASH,
  QUERY_MODULE_HASH,
} from "../generated/typescript/client.js";

const STORE_ID = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b30";
const CUSTOMER_ID = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b31";
const ORDER_ID = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b33";
const [endpoint, credentialFile, riffdbPath] = process.argv.slice(2);
if (endpoint === undefined || credentialFile === undefined || riffdbPath === undefined) {
  throw new Error("usage: orders ENDPOINT CREDENTIAL_FILE RIFFDB_PATH");
}

const transport = new CliApplicationTransport({ endpoint, credentialFile, riffdbPath });
const application = new AgentOrdersClient(transport, 3);

const server = createServer(async (request, response) => {
  if (request.method !== "GET" || request.url !== "/rehearsal") {
    response.writeHead(404).end();
    return;
  }
  try {
    const command = await application.createStore({
      name: "Acme Supply",
      store_id: STORE_ID,
      idempotency_key: "orders-store-acme",
    });
    if (!command.replayed || command.commitSequence === undefined) {
      throw new Error("seed replay did not retain its commit identity");
    }
    const readAfterCommit = command.commitSequence;
    const history = await application.customerHistory(
      { store_id: STORE_ID, customer_id: CUSTOMER_ID, after: null, limit: 25 },
      { readAfterCommit },
    );
    const dashboard = await application.inventoryDashboard(
      { store_id: STORE_ID, after: null, limit: 50 },
      { readAfterCommit },
    );
    const openOrders = await application.openOrders(
      { store_id: STORE_ID, status: "Reserved", after: null, limit: 25 },
      { readAfterCommit },
    );
    const page = await application.orderPage(
      { store_id: STORE_ID, order_id: ORDER_ID },
      { readAfterCommit },
    );
    if (
      history.value.outcome !== "Found"
      || dashboard.value.outcome !== "Found"
      || openOrders.value.outcome !== "Found"
      || page.value.outcome !== "Found"
    ) {
      throw new Error("a generated Orders query did not return Found");
    }
    const applicationHead = [
      history.applicationHead,
      dashboard.applicationHead,
      openOrders.applicationHead,
      page.applicationHead,
    ].reduce((left, right) => left > right ? left : right);
    if (applicationHead < command.commitSequence) {
      throw new Error("read-after-commit evidence regressed");
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      schema: "riffdb-rehearsal-runtime/v1",
      domain: "orders",
      contractLineage: CONTRACT_LINEAGE,
      contractVersion: CONTRACT_VERSION,
      contractBundleHash: CONTRACT_BUNDLE_HASH,
      moduleHash: QUERY_MODULE_HASH,
      command: "CreateStore",
      commandPlanHash: CREATE_STORE_PLAN_HASH,
      query: "OrderPage",
      queryIdentity: page.identity,
      commitSequence: command.commitSequence.toString(),
      applicationHead: applicationHead.toString(),
    }));
  } catch (error) {
    console.error(error);
    response.writeHead(500, { "content-type": "application/json" });
    response.end('{"error":"generated application rehearsal failed"}');
  }
});

server.listen(0, "127.0.0.1", () => {
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("invalid rehearsal HTTP address");
  }
  process.stdout.write(`riffdb-rehearsal-ready-v1\t${address.port}\n`);
});
