import { pathToFileURL } from "node:url";

import { CliApplicationTransport } from "../../../clients/typescript/runtime/dist/index.js";

const [domain, endpoint, credentialFile, riffdbPath, generatedRoot] = process.argv.slice(2);
if (!domain || !endpoint || !credentialFile || !riffdbPath || !generatedRoot) {
  throw new Error("missing generated application acceptance argument");
}
const transport = new CliApplicationTransport({ endpoint, credentialFile, riffdbPath });

if (domain === "agent-blog") {
  const generated = await import(pathToFileURL(
    `${generatedRoot}/agent-blog/generated/typescript/client.js`,
  ));
  const application = new generated.AgentBlogClient(transport, 3);
  const result = await application.postPage({
    site_id: "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b20",
    post_id: "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b22",
    comments_after: null,
  });
  if (result.value.outcome !== "Found") throw new Error("blog page did not return Found");
  process.stdout.write(`${result.value.post.title}\n`);
} else if (domain === "agent-orders") {
  const generated = await import(pathToFileURL(
    `${generatedRoot}/agent-orders/generated/typescript/client.js`,
  ));
  const application = new generated.AgentOrdersClient(transport, 3);
  const result = await application.orderPage({
    store_id: "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b30",
    order_id: "018f0f8b-7c6d-7e31-8a4f-2c2d37a52b33",
  });
  if (result.value.outcome !== "Found") throw new Error("order page did not return Found");
  process.stdout.write(`${result.value.customer.display_name}\n`);
} else {
  throw new Error("unknown generated application domain");
}
