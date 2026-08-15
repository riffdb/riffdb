import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  DRIVER_ERROR_REGISTRY_HASH,
  DRIVER_PROTOCOL_VERSION,
  DRIVER_VALUE_REGISTRY_HASH,
  DriverApplicationTransport,
} from "@riffdb/client";

const HASH = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const identity = {
  applicationManifestHash: HASH,
  operationCatalogHash: HASH,
  contractLineage: "EmptyFrontierAcceptance",
  contractVersion: 1n,
  contractBundleHash: HASH,
  database: "default",
  role: "EmptyFrontierApplication",
  roleDefinitionHash: HASH,
  remoteIdentityHash: HASH,
};

const root = await mkdtemp(join(tmpdir(), "riffdb-installed-empty-frontier-"));
const socketPath = join(root, "driver.sock");
const server = createServer((socket) => {
  let input = Buffer.alloc(0);
  socket.on("data", (chunk) => {
    input = Buffer.concat([input, chunk]);
    while (input.length >= 4) {
      const length = input.readUInt32BE(0);
      if (input.length < length + 4) return;
      const request = JSON.parse(input.subarray(4, length + 4).toString("utf8"));
      input = input.subarray(length + 4);
      const response = request.type === "handshake"
        ? {
            type: "handshake",
            request_id: request.request_id,
            protocol_version: DRIVER_PROTOCOL_VERSION,
            driver_identity: "riffdb-driver-host/v1",
            application_manifest_hash: identity.applicationManifestHash,
            operation_catalog_hash: identity.operationCatalogHash,
            contract_lineage: identity.contractLineage,
            contract_version: 1,
            contract_bundle_hash: identity.contractBundleHash,
            database: identity.database,
            role: identity.role,
            role_definition_hash: identity.roleDefinitionHash,
            remote_identity_hash: identity.remoteIdentityHash,
            value_registry_hash: DRIVER_VALUE_REGISTRY_HASH,
            error_registry_hash: DRIVER_ERROR_REGISTRY_HASH,
          }
        : {
            type: "result",
            request_id: request.request_id,
            value: { type: "record", value: { outcome: { type: "enum", value: "NotFound" } } },
            application_head: 0,
            cursor: null,
            replayed: false,
          };
      const body = Buffer.from(JSON.stringify(response), "utf8");
      const frame = Buffer.alloc(body.length + 4);
      frame.writeUInt32BE(body.length);
      body.copy(frame, 4);
      socket.write(frame);
    }
  });
});

await new Promise((resolve, reject) => {
  server.once("error", reject);
  server.listen(socketPath, resolve);
});
try {
  const transport = await DriverApplicationTransport.connect({ socketPath, identity });
  try {
    const result = await transport.invoke(
      { name: "empty_frontier_find", inputSchemaHash: HASH },
      { key: { type: "string", value: "absent" } },
    );
    assert.equal(result.applicationHead, 0n);
  } finally {
    await transport.shutdown();
  }
} finally {
  await new Promise((resolve) => server.close(resolve));
  await rm(root, { recursive: true, force: true });
}
