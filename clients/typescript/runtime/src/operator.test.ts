import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { join } from "node:path";
import test from "node:test";

import { OperatorTransport } from "./operator.js";

const CAMPAIGN = "018f2f85-3c20-7a31-8f11-112233445566";
const SOURCE = "018f2f85-3c20-7a31-8f11-112233445577";
const TARGET = "018f2f85-3c20-7a31-8f11-112233445588";
const HASH = "0101010101010101010101010101010101010101010101010101010101010101";

test("operator transport is campaign-bound and has no application dispatch", async () => {
  const scratch = process.env.TMPDIR;
  assert.ok(scratch !== undefined && scratch.length > 0, "TMPDIR must name the protected test scratch root");
  const directory = await mkdtemp(join(scratch, "riffdb-ts-operator-"));
  const socketPath = join(directory, "operator.sock");
  const seen: Array<Record<string, unknown>> = [];
  let count = 0;
  const server = createServer((socket) => {
    let input = Buffer.alloc(0);
    socket.on("data", (chunk: Buffer) => {
      input = Buffer.concat([input, chunk]);
      while (input.length >= 4) {
        const length = input.readUInt32BE(0);
        if (input.length < length + 4) return;
        const request = JSON.parse(input.subarray(4, length + 4).toString("utf8")) as Record<string, unknown>;
        input = input.subarray(length + 4);
        assert.equal(Object.hasOwn(request, "operation"), false);
        assert.equal(Object.hasOwn(request, "credential"), false);
        assert.equal(Object.hasOwn(request, "endpoint"), false);
        seen.push(request);
        const requestId = String(request.request_id);
        const response = count === 0
          ? {
              type: "handshake", request_id: requestId, protocol_version: 1,
              driver_identity: "riffdb-driver-host/v1", database: "restored",
              campaign_id: CAMPAIGN, portability_manifest_hash: HASH,
            }
          : count === 1
            ? { type: "operation", request_id: requestId, operation: operation() }
            : { type: "not_found", request_id: requestId };
        count += 1;
        socket.write(frame(response));
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  try {
    const transport = await OperatorTransport.connect(socketPath, {
      database: "restored", campaignId: CAMPAIGN, portabilityManifestHash: HASH,
    });
    const progress = await transport.start("{}", "{}", 3);
    assert.equal(progress.rowsApplied, 2n);
    assert.equal(await transport.status(), undefined);
    await assert.rejects(
      transport.applyPage(newPage({ operationComplete: true, nextCursorBase64: "cursor" })),
      /invalid RiffDB operator page/u,
    );
    assert.deepEqual(seen.map((request) => request.type), ["handshake", "start", "status"]);
    await transport.shutdown();
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

function newPage(overrides: Partial<Parameters<OperatorTransport["applyPage"]>[0]> = {}): Parameters<OperatorTransport["applyPage"]>[0] {
  return {
    exportOperationId: SOURCE,
    pageNumber: 1n,
    canonicalJsonLines: ["{\"entity\":\"Ticket\"}"],
    classComplete: true,
    operationComplete: true,
    pageHashHex: HASH,
    ...overrides,
  };
}

function operation(): Record<string, unknown> {
  return {
    campaign_id: CAMPAIGN,
    contract_lineage: "TicketDesk",
    scope: "whole_application",
    portability_manifest_hash: HASH,
    export_manifest_hash: HASH,
    export_receipt_hash: HASH,
    source_database_id: SOURCE,
    target_database_id: TARGET,
    source_rows: "3",
    source_pages: "1",
    next_page: "2",
    rows_applied: "2",
    phase: "applying",
    failure: null,
    canonical_reimport_receipt_json: null,
    reimport_receipt_hash: null,
  };
}

function frame(value: Record<string, unknown>): Buffer {
  const body = Buffer.from(JSON.stringify(value), "utf8");
  const output = Buffer.allocUnsafe(body.length + 4);
  output.writeUInt32BE(body.length, 0);
  body.copy(output, 4);
  return output;
}
