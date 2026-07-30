import { createServer } from "node:http";

import { CliApplicationTransport } from "@riffdb/application";

import {
  AgentBlogClient,
  CONTRACT_BUNDLE_HASH,
  CONTRACT_LINEAGE,
  CONTRACT_VERSION,
  CREATE_SITE_PLAN_HASH,
  QUERY_MODULE_HASH,
} from "../generated/typescript/client.js";

const SITE_ID = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b20";
const POST_ID = "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b22";
const [endpoint, credentialFile, riffdbPath] = process.argv.slice(2);
if (endpoint === undefined || credentialFile === undefined || riffdbPath === undefined) {
  throw new Error("usage: blog ENDPOINT CREDENTIAL_FILE RIFFDB_PATH");
}

const transport = new CliApplicationTransport({ endpoint, credentialFile, riffdbPath });
const application = new AgentBlogClient(transport, 3);

const server = createServer(async (request, response) => {
  if (request.method !== "GET" || request.url !== "/rehearsal") {
    response.writeHead(404).end();
    return;
  }
  try {
    const command = await application.createSite({
      name: "Acme Engineering",
      site_id: SITE_ID,
      idempotency_key: "blog-site-acme",
    });
    if (!command.replayed || command.commitSequence === undefined) {
      throw new Error("seed replay did not retain its commit identity");
    }
    const readAfterCommit = command.commitSequence;
    const moderation = await application.moderationQueue(
      { site_id: SITE_ID, status: "Approved", after: null, limit: 25 },
      { readAfterCommit },
    );
    const slug = await application.postBySlug(
      { site_id: SITE_ID, slug: "safe-application-data" },
      { readAfterCommit },
    );
    const page = await application.postPage(
      { site_id: SITE_ID, post_id: POST_ID, comments_after: null },
      { readAfterCommit },
    );
    const feed = await application.publicFeed(
      { site_id: SITE_ID, status: "Published", after: null, limit: 20 },
      { readAfterCommit },
    );
    if (
      moderation.value.outcome !== "Found"
      || slug.value.outcome !== "Found"
      || page.value.outcome !== "Found"
      || feed.value.outcome !== "Found"
    ) {
      throw new Error("a generated Blog query did not return Found");
    }
    const applicationHead = [
      moderation.applicationHead,
      slug.applicationHead,
      page.applicationHead,
      feed.applicationHead,
    ].reduce((left, right) => left > right ? left : right);
    if (applicationHead < command.commitSequence) {
      throw new Error("read-after-commit evidence regressed");
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({
      schema: "riffdb-rehearsal-runtime/v1",
      domain: "blog",
      contractLineage: CONTRACT_LINEAGE,
      contractVersion: CONTRACT_VERSION,
      contractBundleHash: CONTRACT_BUNDLE_HASH,
      moduleHash: QUERY_MODULE_HASH,
      command: "CreateSite",
      commandPlanHash: CREATE_SITE_PLAN_HASH,
      query: "PostPage",
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
