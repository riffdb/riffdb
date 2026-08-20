import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const runtime = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_RUNTIME));
const generated = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_GENERATED));
const {
  DriverApplicationError,
  DriverApplicationTransport,
  DriverGeneratedApplicationTransport,
} = runtime;
const { DriverConformanceClient } = generated;

const identityDocument = JSON.parse(await readFile(process.env.RIFFDB_CONFORMANCE_IDENTITY, "utf8"));
const identity = { ...identityDocument, contractVersion: BigInt(identityDocument.contractVersion) };
const driver = await DriverApplicationTransport.connect({
  socketPath: process.env.RIFFDB_CONFORMANCE_SOCKET,
  identity,
});
try {
  const client = new DriverConformanceClient(new DriverGeneratedApplicationTransport(driver), 3);
  const organizationId = "018f0f8b-7c6d-7e31-8a4f-000000000100";
  if (process.env.RIFFDB_CONFORMANCE_EXPECT_REVOKED === "1") {
    let code;
    try {
      await client.itemSecret({ organization_id: organizationId, item_id: "018f0f8b-7c6d-7e31-8a4f-000000000103" });
    } catch (error) {
      if (error instanceof DriverApplicationError) code = error.details.code;
    }
    if (code !== "RDB-AUTH-0215") throw new Error("TypeScript revocation error lost semantic details");
    console.log(JSON.stringify({ schema: "riffdb.driver-conformance-fault/v1", fault: "revocation", code }));
    process.exitCode = 0;
  } else {
  const itemId = "018f0f8b-7c6d-7e31-8a4f-000000000103";
  const idempotencyKey = "driver-conformance-typescript-create-v1";
  const tokenDigest = "typescript-secret-digest-must-not-log";
  const input = { title: "Shared remote TypeScript", token_digest: tokenDigest, item_id: itemId, idempotency_key: idempotencyKey, organization_id: organizationId };
  const first = await client.createItem(input);
  if (first.replayed || first.commitSequence === undefined || first.outcome.outcome !== "Created") {
    throw new Error("first TypeScript command did not create the item");
  }
  const replay = await client.createItem(input);
  if (!replay.replayed) throw new Error("second TypeScript command did not replay");
  const page = await client.itemPage({ organization_id: organizationId, item_id: itemId }, { readAfterCommit: first.commitSequence });
  if (page.value.outcome !== "Found" || page.value.item.item_id !== itemId
      || page.value.item.title !== "Shared remote TypeScript") {
    throw new Error("TypeScript read-after-commit returned the wrong item");
  }
  const secret = await client.itemSecret({ organization_id: organizationId, item_id: itemId }, { readAfterCommit: first.commitSequence });
  const redacted = generated.redactItemSecretResult(secret.value);
  if (secret.value.outcome !== "Found" || secret.value.secret.token_digest !== tokenDigest
      || JSON.stringify(redacted).includes(tokenDigest)) {
    throw new Error("TypeScript secret query value or redaction helper was incorrect");
  }
  let exact;
  for (let attempt = 0; attempt < 200; attempt += 1) {
    try {
      exact = await client.searchItems(
        { organization_id: organizationId, needle: "remote TypeScript", limit: 50, offset: 0n },
        { readAfterCommit: first.commitSequence },
      );
      break;
    } catch (error) {
      if (!(error instanceof DriverApplicationError)
          || (error.details.code !== "RDB-QUERY-0102" && error.details.code !== "RDB-PROJECTION-0103")) {
        throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
  }
  if (exact === undefined) throw new Error("TypeScript exact provider did not become ready within the retry bound");
  if (exact.value.outcome !== "Found" || exact.value.total.value !== 1n
      || exact.value.items.length !== 1 || exact.value.items[0].item_id !== itemId
      || exact.value.items[0].organization_id !== organizationId) {
    throw new Error("TypeScript exact page and whole-population total diverged");
  }
  let reuseError;
  try {
    await client.createItem({ ...input, title: "Changed input" });
  } catch (error) {
    if (error instanceof DriverApplicationError) reuseError = error.details.code;
  }
  if (reuseError !== "RDB-COMMAND-0101") throw new Error("TypeScript reuse error lost semantic details");
  console.log(JSON.stringify({
    schema: "riffdb.driver-conformance-observation/v1", language: "typescript",
    created: "Created", replayed: true, query: "Found", read_after_commit: true,
    reuse_error: reuseError, secret_query: "Found", secret_redacted: true,
    exact_query: "Found", exact_total: 1,
  }));
  }
} finally {
  await driver.shutdown();
}
