import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const runtime = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_RUNTIME));
const generated = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_GENERATED));
const { DriverApplicationTransport, DriverGeneratedApplicationTransport } = runtime;
const { AdapterBulkConformanceClient } = generated;

const identityDocument = JSON.parse(await readFile(process.env.RIFFDB_CONFORMANCE_IDENTITY, "utf8"));
const identity = { ...identityDocument, contractVersion: BigInt(identityDocument.contractVersion) };
const driver = await DriverApplicationTransport.connect({
  socketPath: process.env.RIFFDB_CONFORMANCE_SOCKET,
  identity,
});

const id = (suffix) => `018f0f8b-7c6d-7e31-8a4f-00000000${suffix.toString(16).padStart(4, "0")}`;
const assert = (condition, label) => { if (!condition) throw new Error(`TypeScript adapter assertion: ${label}`); };

try {
  const client = new AdapterBulkConformanceClient(new DriverGeneratedApplicationTransport(driver), 3);
  let bounded = false;
  try {
    await client.writeTuples({ tuples: [], request_id: id(512) });
  } catch (error) {
    bounded = String(error).includes("list input");
  }
  assert(bounded, "empty collection preflight");

  const tuples = {
    request_id: id(513),
    tuples: [{ store_id: id(514), tuple_id: id(515), object: "document:roadmap", relation: "viewer", subject: "user:agent" }],
  };
  const firstTuple = await client.writeTuples(tuples);
  assert(firstTuple.outcome.outcome === "TuplesWritten", "OpenFGA outcome");
  assert((await client.writeTuples(tuples)).replayed, "OpenFGA replay");

  const metrics = {
    request_id: id(516),
    metrics: [{ experiment_id: id(517), metric_id: id(518), name: "latency", step: 1n, value_micros: 125n }],
  };
  assert((await client.logMetrics(metrics)).outcome.outcome === "MetricsLogged", "MLflow outcome");
  assert((await client.logMetrics(metrics)).replayed, "MLflow replay");

  const documents = {
    request_id: id(519),
    documents: [{ site_id: id(520), document_id: id(521), revision_id: id(522), title: "Document", body: "bounded body" }],
  };
  assert((await client.createDocumentGraphs(documents)).outcome.outcome === "DocumentGraphsCreated", "Payload outcome");
  assert((await client.createDocumentGraphs(documents)).replayed, "Payload replay");

  const pipelines = {
    request_id: id(523),
    pipelines: [{ organization_id: id(524), pipeline_id: id(525), step_id: id(526), name: "verify", run_text: "npm test" }],
  };
  assert((await client.createPipelinesWithSteps(pipelines)).outcome.outcome === "PipelinesCreated", "Woodpecker outcome");
  assert((await client.createPipelinesWithSteps(pipelines)).replayed, "Woodpecker replay");

  console.log(JSON.stringify({
    schema: "riffdb.adapter-bulk-observation/v1",
    language: "typescript",
    bounded_preflight: true,
    replayed: true,
    adapters: ["mlflow", "openfga", "payload", "woodpecker"],
  }));
} finally {
  await driver.shutdown();
}
