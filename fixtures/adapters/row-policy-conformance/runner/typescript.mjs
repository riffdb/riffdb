import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const runtime = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_RUNTIME));
const generated = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_GENERATED));
const { DriverApplicationError, DriverApplicationTransport, DriverGeneratedApplicationTransport } = runtime;
const { AdapterRowPolicyConformanceClient } = generated;

const identityDocument = JSON.parse(await readFile(process.env.RIFFDB_CONFORMANCE_IDENTITY, "utf8"));
const identity = { ...identityDocument, contractVersion: BigInt(identityDocument.contractVersion) };
const driver = await DriverApplicationTransport.connect({
  socketPath: process.env.RIFFDB_CONFORMANCE_SOCKET,
  identity,
});
const id = (suffix) => `018f0f8b-7c6d-7e31-8a4f-0000000000${suffix.toString(16).padStart(2, "0")}`;
const assert = (condition, label) => { if (!condition) throw new Error(`TypeScript row-policy assertion: ${label}`); };
const mode = process.env.RIFFDB_ROW_POLICY_MODE;
const expected = mode === "owner"
  ? { documents: 6, drafts: 4, experiments: 3, metrics: 3, runVisible: true }
  : mode === "outsider"
    ? { documents: 3, drafts: 2, experiments: 1, metrics: 1, runVisible: false }
    : null;
if (!expected) throw new Error("unknown row-policy mode");

try {
  const client = new AdapterRowPolicyConformanceClient(new DriverGeneratedApplicationTransport(driver), 3);
  const principalId = id(mode === "owner" ? 1 : 3);
  const documentSuffix = mode === "owner" ? 60 : 61;
  const requestSuffix = mode === "owner" ? 160 : 161;
  const created = await client.createDocument({
    body: "created through every generated language", state: "Draft", title: `Document shared-${mode}`,
    group_id: null, owner_id: principalId, request_id: id(requestSuffix), visibility: "Private",
    document_id: id(documentSuffix), organization_id: id(10),
  });
  assert(created.outcome.outcome === "DocumentCreated", "typed protected command outcome");
  const documents = await client.listDocuments({ organization_id: id(10) });
  assert(documents.value.outcome === "Found" && documents.value.documents.length === expected.documents, "document page");
  const drafts = await client.listDraftDocuments({ organization_id: id(10) });
  assert(drafts.value.outcome === "Found" && drafts.value.documents.length === expected.drafts, "draft page");
  const search = await client.searchDocuments({ organization_id: id(10), title_prefix: "Document" });
  assert(search.value.outcome === "Found" && search.value.documents.length === expected.documents, "text search");
  const detail = await client.getDocument({ organization_id: id(10), document_id: id(15) });
  assert((detail.value.outcome === "Found") === (mode === "owner"), "document detail");
  const documentSummary = await client.documentSummary({ organization_id: id(10) });
  assert(documentSummary.value.outcome === "Found", "document summary outcome");
  assert(documentSummary.value.summary.reduce((count, group) => count + group.document_count, 0n) === BigInt(expected.documents), "document aggregate");
  const experiments = await client.listExperiments({ organization_id: id(10) });
  assert(experiments.value.outcome === "Found" && experiments.value.experiments.length === expected.experiments, "experiment page");
  const dashboard = await client.metricDashboard({ organization_id: id(10) });
  assert(dashboard.value.outcome === "Found" && dashboard.value.summary.length === 1, "dashboard");
  assert(dashboard.value.summary[0].sample_count === BigInt(expected.metrics), "policy-before-aggregate count");
  const run = await client.runPage({ organization_id: id(10), experiment_id: id(23), run_id: id(31) });
  assert((run.value.outcome === "Found") === expected.runVisible, "run visibility");
  if (run.value.outcome === "Found") {
    assert(run.value.metrics.length === 1 && run.value.artifacts.length === 1, "nested policy hydration");
  }
  let transferCode;
  try {
    await client.attemptDocumentTransfer({
      request_id: id(requestSuffix + 10), document_id: id(documentSuffix),
      new_owner_id: id(2), organization_id: id(10),
    });
  } catch (error) {
    if (error instanceof DriverApplicationError) transferCode = error.details.code;
  }
  assert(transferCode === "RDB-AUTH-0214", "typed successor-row authorization error");
  const lifecycleChecked = mode === "owner";
  if (lifecycleChecked) {
    const finished = await client.finishRun({
      run_id: id(33), request_id: id(180), experiment_id: id(21),
      organization_id: id(10), expected_revision: 1n,
    });
    assert(finished.outcome.outcome === "RunFinished", "revision-checked MLflow transition");
    const stale = await client.finishRun({
      run_id: id(33), request_id: id(181), experiment_id: id(21),
      organization_id: id(10), expected_revision: 1n,
    });
    assert(stale.outcome.outcome === "FinishStale", "stale MLflow transition");
  }
  console.log(JSON.stringify({
    schema: "riffdb.adapter-row-policy-observation/v1", language: "typescript", mode,
    documents: expected.documents, drafts: expected.drafts, experiments: expected.experiments, metrics: expected.metrics,
    group_run_visible: expected.runVisible, policy_before_aggregate: true, detail_and_search: true,
    nested_policy: true, protected_command: true, successor_escape_denied: true,
    lifecycle_checked: lifecycleChecked,
  }));
} finally {
  await driver.shutdown();
}
