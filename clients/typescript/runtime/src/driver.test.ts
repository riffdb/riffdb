import assert from "node:assert/strict";
import { mkdir, readFile, rm } from "node:fs/promises";
import { createServer, type Socket } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  DRIVER_ERROR_REGISTRY_HASH,
  DRIVER_PROTOCOL_VERSION,
  DRIVER_VALUE_REGISTRY_HASH,
  DriverApplicationError,
  DriverApplicationTransport,
  exactDecimal,
  exactMoney,
  type DriverApplicationIdentity,
} from "./driver.js";
import { DriverGeneratedApplicationTransport } from "./index.js";

const HASH = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
let fixtureSequence = 0;
const identity: DriverApplicationIdentity = {
  applicationManifestHash: HASH,
  operationCatalogHash: HASH,
  contractLineage: "TicketDesk",
  contractVersion: 5n,
  contractBundleHash: HASH,
  database: "default",
  role: "TicketDeskApplication",
  roleDefinitionHash: HASH,
  remoteIdentityHash: HASH,
};

test("exact decimal helpers never pass business values through number", () => {
  assert.deepEqual(exactMoney("USD", "25.00"), {
    currency: "USD",
    amount: { coefficientTwosComplement: Uint8Array.of(0x09, 0xc4), scale: 2, precision: 38 },
  });
  assert.deepEqual(exactDecimal("-1.29", 4, 2), {
    coefficientTwosComplement: Uint8Array.of(0xff, 0x7f), scale: 2, precision: 4,
  });
  assert.throws(() => exactMoney("usd", "1.00"), /currency/);
});

test("one retained session handshakes once and multiplexes exact operations", async () => {
  const fixture = await DriverFixture.start((request) => {
    if (request.type === "invoke") {
      return {
        type: "result", request_id: request.request_id,
        value: { type: "record", value: { outcome: { type: "enum", value: "Found" } } },
        application_head: 7, cursor: null, replayed: false,
      };
    }
    return undefined;
  });
  try {
    const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
    const operation = { name: "ticketdesk_get_ticket", inputSchemaHash: HASH };
    const input = { ticket_id: { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c998" } } as const;
    const [first, second] = await Promise.all([
      transport.invoke(operation, input), transport.invoke(operation, input),
    ]);
    assert.equal(first.applicationHead, 7n);
    assert.equal(second.applicationHead, 7n);
    assert.equal(fixture.connections, 1);
    assert.equal(fixture.handshakes, 1);
    assert.equal(fixture.invocations, 2);
    await transport.shutdown();
  } finally {
    await fixture.close();
  }
});

test("concurrent sessions use distinct driver request identities", async () => {
  const requestIds = new Set<string>();
  const fixture = await DriverFixture.start((request) => {
    if (request.type !== "invoke") return undefined;
    requestIds.add(String(request.request_id));
    return {
      type: "result", request_id: request.request_id,
      value: { type: "null" }, application_head: null, cursor: null, replayed: false,
    };
  });
  try {
    const [first, second] = await Promise.all([
      DriverApplicationTransport.connect({ socketPath: fixture.path, identity }),
      DriverApplicationTransport.connect({ socketPath: fixture.path, identity }),
    ]);
    const operation = { name: "ticketdesk_get_ticket", inputSchemaHash: HASH };
    await Promise.all([first.invoke(operation, {}), second.invoke(operation, {})]);
    assert.equal(requestIds.size, 2);
    await Promise.all([first.shutdown(), second.shutdown()]);
  } finally {
    await fixture.close();
  }
});

test("u64 frontiers remain exact across the JSON number protocol", async () => {
  const maximum = 18_446_744_073_709_551_615n;
  const golden = (await readFile(
    new URL("../../../../fixtures/driver/v1/max-u64-result.json", import.meta.url),
    "utf8",
  )).trim();
  const fixture = await DriverFixture.start((request, socket) => {
    if (request.type !== "invoke") return undefined;
    writeRawFrame(socket, golden.replace('"max-u64"', JSON.stringify(request.request_id)));
    return undefined;
  });
  try {
    const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
    const result = await transport.invoke(
      { name: "ticketdesk_after_commit", inputSchemaHash: HASH },
      {},
      { readAfterCommit: maximum },
    );
    assert.equal(result.applicationHead, maximum);
    assert.match(fixture.lastRequest, /"read_after_commit":18446744073709551615/);
    assert.doesNotMatch(fixture.lastRequest, /"read_after_commit":"18446744073709551615"/);
    await transport.shutdown();
  } finally {
    await fixture.close();
  }
});

test("batch results preserve commit identity and durable outcome locator", async () => {
  const fixture = await DriverFixture.start((request) => request.type === "batch" ? {
    type: "batch_result", request_id: request.request_id, checkpoint: 1, total: 1,
    items: [{
      index: 0,
      outcome: {
        type: "result", value: { type: "enum", value: "Created" },
        commit_sequence: 9, outcome_uri: "riffdb://outcomes/create/one", replayed: true,
      },
    }],
  } : undefined);
  try {
    const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
    const result = await transport.batch(
      { name: "ticketdesk_create_comment", inputSchemaHash: HASH },
      [{ body: { type: "string", value: "hello" } }],
      1,
      0,
    );
    assert.equal(result.items[0]?.result?.commitSequence, 9n);
    assert.equal(result.items[0]?.result?.outcomeUri, "riffdb://outcomes/create/one");
    assert.equal(result.items[0]?.result?.replayed, true);
    await transport.shutdown();
  } finally {
    await fixture.close();
  }
});

test("generated facade adapter owns value assembly and typed page decoding", async () => {
  const fixture = await DriverFixture.start((request) => {
    if (request.type !== "invoke") return undefined;
    assert.equal(request.operation, "ticketdesk_get_ticket");
    assert.deepEqual(request.input, {
      organization_id: { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c998" },
    });
    return {
      type: "result", request_id: request.request_id,
      value: {
        type: "record",
        value: {
          outcome: { type: "enum", value: "Found" },
          ticket: {
            type: "record",
            value: {
              ticket_id: { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c999" },
              title: { type: "string", value: "Cannot sign in" },
            },
          },
        },
      },
      application_head: 11, cursor: "next-page", replayed: false,
    };
  });
  try {
    const driver = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
    const transport = new DriverGeneratedApplicationTransport(driver);
    const result = await transport.executeNamedQuery({
      driverOperation: { name: "ticketdesk_get_ticket", inputSchemaHash: HASH },
      contractLineage: "TicketDesk", contractVersion: 5,
      contractBundleHash: HASH, moduleHash: HASH, queryName: "GetTicket", planHash: HASH,
      parameters: { organization_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c998" },
      parameterSchema: { kind: "record", fields: [{ name: "organization_id", schema: { kind: "uuid" } }] },
      resultSchemas: {
        Found: {
          kind: "record",
          fields: [{
            name: "ticket",
            schema: { kind: "record", fields: [
              { name: "ticket_id", schema: { kind: "uuid" } },
              { name: "title", schema: { kind: "string" } },
            ] },
          }],
        },
      },
      decodeError: () => new Error("unexpected"),
    });
    assert.deepEqual(result.value, {
      outcome: "Found",
      ticket: {
        ticket_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c999",
        title: "Cannot sign in",
      },
    });
    assert.equal(result.applicationHead, 11n);
    assert.equal(result.nextCursor, "next-page");
    await driver.shutdown();
  } finally {
    await fixture.close();
  }
});

test("generated reactive facade retains cursors and lease evidence on one session", async () => {
  let watchCalls = 0;
  const fixture = await DriverFixture.start((request) => {
    if (request.type !== "invoke") return undefined;
    if (request.operation === "ticket_events_next") {
      const input = request.input as Record<string, unknown>;
      assert.deepEqual(input.batch_limit, { type: "u64", value: "4" });
      assert.deepEqual(input.in_flight_limit, { type: "u64", value: "8" });
      assert.deepEqual(input.lease_seconds, { type: "u64", value: "90" });
      return {
        type: "result", request_id: request.request_id,
        value: { type: "record", value: {
          events: { type: "list", value: [{ type: "record", value: {
            event_id: { type: "string", value: "12:0" },
            event_name: { type: "enum", value: "TicketCreated" },
            writer_contract_version: { type: "u64", value: "5" },
            command_name: { type: "string", value: "CreateTicket" },
            actor_kind: { type: "enum", value: "application" },
            provenance_uri: { type: "string", value: "riffdb://provenance/12" },
            history_incarnation: { type: "u64", value: "2" },
            fields: { type: "record", value: { title: { type: "string", value: "Cannot sign in" } } },
            attempt: { type: "u64", value: "1" },
            lease_token: { type: "string", value: "bGVhc2U=" },
            expires_at: { type: "timestamp", value: { seconds: "99", nanos: 4 } },
          } }] },
          status: driverStatus(), wait_timed_out: { type: "bool", value: false },
        } },
        application_head: null, cursor: null, replayed: false,
      };
    }
    if (request.operation === "ticket_events_ack") {
      const input = request.input as Record<string, unknown>;
      assert.deepEqual(input.event_id, { type: "string", value: "12:0" });
      assert.deepEqual(input.history_incarnation, { type: "u64", value: "2" });
      return { type: "result", request_id: request.request_id, value: { type: "enum", value: "applied" }, application_head: null, cursor: null, replayed: false };
    }
    if (request.operation === "ticket_events_nack") {
      const input = request.input as Record<string, unknown>;
      assert.deepEqual(input.retry_delay_nanos, { type: "u64", value: "25000000" });
      return { type: "result", request_id: request.request_id, value: { type: "enum", value: "applied" }, application_head: null, cursor: null, replayed: false };
    }
    if (request.operation === "ticket_page_watch") {
      watchCalls += 1;
      if (watchCalls === 1) {
        return {
          type: "result", request_id: request.request_id,
          value: { type: "record", value: {
            kind: { type: "enum", value: "snapshot" },
            result: { type: "record", value: { outcome: { type: "enum", value: "Found" } } },
            checkpoint: { type: "record", value: { history_incarnation: { type: "u64", value: "2" }, application_head: { type: "u64", value: "12" }, cursor: { type: "string", value: "cursor-12" } } },
          } },
          application_head: 12, cursor: "cursor-12", replayed: false,
        };
      }
      const options = request.options as Record<string, unknown>;
      assert.equal(options.cursor, "cursor-12");
      return {
        type: "result", request_id: request.request_id,
        value: { type: "record", value: { kind: { type: "enum", value: "terminal" }, reason: { type: "enum", value: "authorization_changed" }, last_frontier: { type: "null" } } },
        application_head: null, cursor: null, replayed: false,
      };
    }
    return undefined;
  });
  try {
    const driver = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
    const transport = new DriverGeneratedApplicationTransport(driver);
    const request = {
      driverOperations: {
        next: { name: "ticket_events_next", inputSchemaHash: HASH },
        ack: { name: "ticket_events_ack", inputSchemaHash: HASH },
        nack: { name: "ticket_events_nack", inputSchemaHash: HASH },
      },
      reactiveModuleHash: HASH,
      operationName: "TicketEvents",
      parameters: { organization_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c998" },
      parameterSchema: { kind: "record", fields: [{ name: "organization_id", schema: { kind: "uuid" } }] } as const,
      consumerName: "ticket-worker",
    };
    const controller = new AbortController();
    const iterator = transport.consumeEventStream<typeof request.parameters, { readonly type: string; readonly title: string }>(
      request,
      { batchLimit: 4, inFlightLimit: 8, leaseSeconds: 90, maximumWaitMs: 1, signal: controller.signal },
    )[Symbol.asyncIterator]();
    const first = await iterator.next();
    assert.equal(first.value?.events[0]?.event.title, "Cannot sign in");
    const delivery = first.value?.events[0];
    assert.ok(delivery !== undefined);
    assert.equal(await transport.acknowledgeEvent(request, delivery), "applied");
    assert.equal(await transport.negativeAcknowledgeEvent(request, delivery, 25), "applied");
    controller.abort();
    assert.equal((await iterator.next()).done, true);

    const watch = transport.watchNamedQuery<{ readonly organization_id: string }, { readonly outcome: string }>({
      driverOperations: { watch: { name: "ticket_page_watch", inputSchemaHash: HASH } },
      reactiveModuleHash: HASH,
      operationName: "TicketPageWatch",
      parameters: request.parameters,
      parameterSchema: request.parameterSchema,
    })[Symbol.asyncIterator]();
    const snapshot = await watch.next();
    assert.equal(snapshot.value?.type, "snapshot");
    assert.equal(snapshot.value?.cursor, "cursor-12");
    const terminal = await watch.next();
    assert.equal(terminal.value?.type, "terminal");
    assert.equal((await watch.next()).done, true);
    assert.equal(fixture.connections, 1);
    await driver.shutdown();
  } finally {
    await fixture.close();
  }
});

test("abort propagates to the exact request and preserves command uncertainty", async () => {
  let blockedRequest: Record<string, unknown> | undefined;
  let blockedSocket: Socket | undefined;
  const fixture = await DriverFixture.start((request, socket) => {
    if (request.type === "invoke") { blockedRequest = request; blockedSocket = socket; return undefined; }
    if (request.type === "cancel" && blockedRequest !== undefined && blockedSocket !== undefined) {
      writeFrame(blockedSocket, {
        type: "error", request_id: blockedRequest.request_id,
        code: "RDB-APP-0002", category: "control", operation: "ticketdesk_create_comment",
        symbol_path: [], contract_lineage: "TicketDesk", contract_version: 5,
        trace_id: null, incident_id: null, message: "application request was cancelled",
        retryability: "not_retryable", recovery_action: "resolve_with_same_idempotency_key",
        outcome_uncertain: true,
      });
      return {
        type: "cancelled", request_id: request.request_id,
        target_request_id: request.target_request_id, terminal: false, outcome_uncertain: true,
      };
    }
    return undefined;
  });
  try {
    const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
    const controller = new AbortController();
    const result = transport.invoke(
      { name: "ticketdesk_create_comment", inputSchemaHash: HASH },
      { body: { type: "string", value: "hello" } },
      { signal: controller.signal },
    );
    controller.abort();
    await assert.rejects(result, (error: unknown) => {
      assert.ok(error instanceof DriverApplicationError);
      assert.equal(error.details.code, "RDB-APP-0002");
      assert.equal(error.details.outcomeUncertain, true);
      assert.equal(error.details.recoveryAction, "resolve_with_same_idempotency_key");
      return true;
    });
    assert.equal(fixture.cancellations, 1);
    await transport.shutdown();
  } finally {
    await fixture.close();
  }
});

test("configuration has no remote endpoint or credential surface", async () => {
  await assert.rejects(
    DriverApplicationTransport.connect({ socketPath: "relative.sock", identity }),
    /socket path/,
  );
  const source = await import("node:fs/promises").then(({ readFile }) =>
    readFile(new URL("./driver.js", import.meta.url), "utf8"));
  for (const forbidden of ["child_process", "credentialFile", "riffdbPath", "grpc", "Authorization: Bearer"]) {
    assert.equal(source.includes(forbidden), false, `runtime contains ${forbidden}`);
  }
});

type Handler = (request: Record<string, unknown>, socket: Socket) => Record<string, unknown> | undefined;

class DriverFixture {
  public connections = 0;
  public handshakes = 0;
  public invocations = 0;
  public cancellations = 0;
  public lastRequest = "";

  private constructor(
    public readonly path: string,
    private readonly root: string,
    private readonly server: ReturnType<typeof createServer>,
  ) {}

  public static async start(handler: Handler): Promise<DriverFixture> {
    fixtureSequence += 1;
    const root = join(homedir(), "tmp", `riffdb-ts-driver-${process.pid}-${fixtureSequence}`);
    await mkdir(root, { recursive: true, mode: 0o700 });
    const path = join(root, "driver.sock");
    let fixture: DriverFixture;
    const server = createServer((socket) => {
      fixture.connections += 1;
      let input = Buffer.alloc(0);
      socket.on("data", (chunk: Buffer) => {
        input = Buffer.concat([input, chunk]);
        while (input.length >= 4) {
          const length = input.readUInt32BE(0);
          if (input.length < length + 4) return;
          fixture.lastRequest = input.subarray(4, length + 4).toString("utf8");
          const request = JSON.parse(fixture.lastRequest) as Record<string, unknown>;
          input = input.subarray(length + 4);
          if (request.type === "handshake") {
            fixture.handshakes += 1;
            writeFrame(socket, {
              type: "handshake", request_id: request.request_id,
              protocol_version: DRIVER_PROTOCOL_VERSION, driver_identity: "riffdb-driver-host/v1",
              application_manifest_hash: identity.applicationManifestHash,
              operation_catalog_hash: identity.operationCatalogHash,
              contract_lineage: identity.contractLineage, contract_version: Number(identity.contractVersion),
              contract_bundle_hash: identity.contractBundleHash, database: identity.database,
              role: identity.role, role_definition_hash: identity.roleDefinitionHash,
              remote_identity_hash: identity.remoteIdentityHash,
              value_registry_hash: DRIVER_VALUE_REGISTRY_HASH,
              error_registry_hash: DRIVER_ERROR_REGISTRY_HASH,
            });
            continue;
          }
          if (request.type === "invoke") fixture.invocations += 1;
          if (request.type === "cancel") fixture.cancellations += 1;
          const response = handler(request, socket);
          if (response !== undefined) writeFrame(socket, response);
        }
      });
    });
    fixture = new DriverFixture(path, root, server);
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(path, () => resolve());
    });
    return fixture;
  }

  public async close(): Promise<void> {
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
    await rm(this.root, { recursive: true, force: true });
  }
}

function writeFrame(socket: Socket, value: Record<string, unknown>): void {
  writeRawFrame(socket, JSON.stringify(value));
}

function writeRawFrame(socket: Socket, value: string): void {
  const body = Buffer.from(value, "utf8");
  const frame = Buffer.alloc(body.length + 4);
  frame.writeUInt32BE(body.length);
  body.copy(frame, 4);
  socket.write(frame);
}

function driverStatus(): Record<string, unknown> {
  return { type: "record", value: {
    revision: { type: "u64", value: "1" },
    checkpoint: { type: "string", value: "12:0" },
    history_incarnation: { type: "u64", value: "2" },
    live_leases: { type: "u64", value: "1" },
    retries: { type: "u64", value: "0" },
    dead_letters: { type: "u64", value: "0" },
  } };
}
