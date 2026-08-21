import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const runtime = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_RUNTIME));
const generated = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_GENERATED));
const { DriverApplicationTransport, DriverGeneratedApplicationTransport } = runtime;
const { AdapterOperationalConformanceClient } = generated;

const identityDocument = JSON.parse(await readFile(process.env.RIFFDB_CONFORMANCE_IDENTITY, "utf8"));
const identity = { ...identityDocument, contractVersion: BigInt(identityDocument.contractVersion) };
const driver = await DriverApplicationTransport.connect({
  socketPath: process.env.RIFFDB_CONFORMANCE_SOCKET,
  identity,
});
const id = (suffix) => `018f0f8b-7c6d-7e31-8a4f-00000000${suffix.toString(16).padStart(4, "0")}`;
const assert = (condition, label) => { if (!condition) throw new Error(`TypeScript adapter assertion: ${label}`); };

try {
  const client = new AdapterOperationalConformanceClient(new DriverGeneratedApplicationTransport(driver), 3);
  const tuples = await client.listFgaTuples({ store_id: id(10), relation: "viewer" });
  assert(tuples.value.outcome === "Found" && tuples.value.tuples.length === 1, "OpenFGA optional relation page");

  const dashboard = await client.metricDashboard({ experiment_id: id(20) });
  assert(dashboard.value.outcome === "Found" && dashboard.value.summary.length === 1, "MLflow aggregate page");
  assert(dashboard.value.summary[0].sample_count === 2n, "MLflow exact count");
  assert(dashboard.value.summary[0].minimum_micros === 125n && dashboard.value.summary[0].maximum_micros === 175n, "MLflow min/max");

  const documents = await client.searchDocuments({ site_id: id(30), title_prefix: "Alpha" });
  assert(documents.value.outcome === "Found" && documents.value.documents.length === 2, "Payload binary prefix page");
  const drafts = await client.listDraftDocuments({ site_id: id(30) });
  assert(drafts.value.outcome === "Found" && drafts.value.documents.length === 1, "Payload null predicate page");

  const contains = await client.exactDocumentsContainsAsc({ site_id: id(30), needle: "Alpha", limit: 1, offset: 1n });
  assert(contains.value.outcome === "Found" && contains.value.total.value === 2n, "generic contains exact total");
  assert(contains.value.documents.length === 1 && contains.value.documents[0].title === "Alpha Published", "generic contains numeric offset");
  const startsWith = await client.exactDocumentsStartsWithAsc({ site_id: id(30), needle: "Alpha", document_id: id(31), limit: 25, offset: 0n });
  assert(startsWith.value.outcome === "Found" && startsWith.value.total.value === 1n, "generic starts-with typed optional filter");
  assert(startsWith.value.documents.length === 1 && startsWith.value.documents[0].document_id === id(31), "generic starts-with result");
  const endsWith = await client.exactDocumentsEndsWithDesc({ site_id: id(30), needle: "Guide", limit: 1, offset: 1n });
  assert(endsWith.value.outcome === "Found" && endsWith.value.total.value === 2n, "generic ends-with exact total");
  assert(endsWith.value.documents.length === 1 && endsWith.value.documents[0].title === "Beta Guide", "generic ends-with descending ordinal");

  const pipelines = await client.listPipelines({ organization_id: id(40), state: "queued" });
  assert(pipelines.value.outcome === "Found" && pipelines.value.pipelines.length === 1, "Woodpecker optional state page");

  const authSession = await client.getAuthSession({ organization_id: id(50), user_id: id(51), session_id: id(52) });
  assert(authSession.value.outcome === "Found", "Better Auth session page");
  assert(authSession.value.state === undefined, "Better Auth result remains page-shaped");
  assert(authSession.value.session.state === "AuthActive" && authSession.value.session.expires_at.seconds === 1800000000n, "Better Auth typed session graph");

  console.log(JSON.stringify({
    schema: "riffdb.adapter-operational-observation/v1",
    language: "typescript",
    catalog_preflight: true,
    optional_filters: true,
    stable_cursor: true,
    null_predicate: true,
    binary_prefix: true,
    exact_aggregates: true,
    exact_text_family: true,
    exact_total: true,
    numeric_offset: true,
    adapters: ["mlflow", "openfga", "better-auth", "woodpecker"],
    regression_adapters: ["payload"],
  }));
} finally {
  await driver.shutdown();
}
