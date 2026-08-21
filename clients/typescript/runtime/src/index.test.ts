import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { CliApplicationTransport, exactDecimal, exactMoney } from "./index.js";

test("generated collection schemas reject counts before transport", async () => {
  const transport = new CliApplicationTransport({
    riffdbPath: "/does-not-exist/riffdb",
    endpoint: "http://127.0.0.1:7443",
    credentialFile: "/does-not-exist/credential",
  });
  await assert.rejects(
    transport.executeCommand({
      contractLineage: "BulkContract",
      contractVersion: 1,
      commandName: "WriteTuples",
      planHash: "a".repeat(64),
      input: { request_id: "01900000-0000-7000-8000-000000000001", tuples: [] },
      idempotencyKey: "01900000-0000-7000-8000-000000000001",
      inputSchema: {
        kind: "record",
        fields: [
          { name: "request_id", schema: { kind: "uuid" } },
          { name: "tuples", schema: { kind: "list", minimum: 1, maximum: 128, value: { kind: "record", fields: [] } } },
        ],
      },
      outcomeSchemas: { Written: { kind: "record", fields: [] } },
      decodeError: () => new Error("unexpected application error"),
    }, 1),
    /invalid generated application input/,
  );
});

test("exact decimal helpers remove handwritten coefficient encoding", () => {
  assert.deepEqual(exactMoney("USD", "25.00"), {
    currency: "USD",
    amount: {
      coefficientTwosComplement: Uint8Array.of(0x09, 0xc4),
      scale: 2,
      precision: 38,
    },
  });
  assert.deepEqual(exactDecimal("-1.29", 4, 2), {
    coefficientTwosComplement: Uint8Array.of(0xff, 0x7f),
    scale: 2,
    precision: 4,
  });
  assert.deepEqual(exactDecimal("0", 1, 0).coefficientTwosComplement, Uint8Array.of(0));
  assert.throws(() => exactMoney("usd", "1.00"), /invalid exact money currency/);
  assert.throws(() => exactDecimal("1.001", 4, 2), /exceeds scale/);
  assert.throws(() => exactDecimal("100.00", 4, 2), /exceeds precision/);
});

test("transport configuration is bounded before any process is started", () => {
  assert.throws(
    () => new CliApplicationTransport({ riffdbPath: "", endpoint: "http://127.0.0.1:1", credentialFile: "credential" }),
    /invalid RiffDB application transport configuration/,
  );
});

test("generated scalar schemas encode losslessly through the symbolic CLI boundary", async () => {
  const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-scalars-"));
  const executable = join(directory, "riffdb-fake.cjs");
  await writeFile(executable, `#!/usr/bin/env node
const fs = require("node:fs");
const inputPath = process.argv[process.argv.indexOf("--input") + 1];
const encoded = fs.readFileSync(inputPath, "utf8").trim();
process.stdout.write(JSON.stringify({
  schema: "riffdb.cli.output/v1",
  ok: true,
  result: {
    plan_hash: "${"a".repeat(64)}",
    outcome_type: "Accepted",
    outcome: {
      type: "record",
      fields: [{ field_id: 1, value: { type: "string", value: encoded } }],
    },
    contract_version: "1",
    status: "executed",
  },
}) + "\\n");
`);
  await chmod(executable, 0o700);
  try {
    const transport = new CliApplicationTransport({
      riffdbPath: executable,
      endpoint: "http://127.0.0.1:7443",
      credentialFile: join(directory, "credential"),
    });
    const result = await transport.executeCommand({
      contractLineage: "ScalarContract",
      contractVersion: 1,
      commandName: "ScalarCommand",
      planHash: "a".repeat(64),
      input: {
        signed: -(1n << 63n),
        unsigned: (1n << 64n) - 1n,
        decimal: {
          coefficientTwosComplement: Uint8Array.of(123),
          scale: 2,
          precision: 3,
        },
        money: {
          currency: "USD",
          amount: { coefficientTwosComplement: Uint8Array.of(123), scale: 2, precision: 3 },
        },
        bytes: Uint8Array.from([115, 97, 102, 101]),
        date: 1,
        timestamp: { seconds: -1n, nanos: 999_999_999 },
        embedding: [0, 1.5, -2.25, 0.5],
      },
      idempotencyKey: "scalar-command",
      inputSchema: {
        kind: "record",
        fields: [
          { name: "signed", schema: { kind: "i64" } },
          { name: "unsigned", schema: { kind: "u64" } },
          { name: "decimal", schema: { kind: "decimal", precision: 3, scale: 2 } },
          { name: "money", schema: { kind: "money", currency: "USD", precision: 3, scale: 2 } },
          { name: "bytes", schema: { kind: "bytes" } },
          { name: "date", schema: { kind: "date" } },
          { name: "timestamp", schema: { kind: "timestamp" } },
          { name: "embedding", schema: { kind: "vector", dimension: 4 } },
        ],
      },
      outcomeSchemas: {
        Accepted: {
          kind: "record",
          fields: [{ name: "encoded", schema: { kind: "string" }, wireId: 1 }],
        },
      },
      decodeError: () => new Error("unexpected application error"),
    }, 3);
    const encoded = JSON.parse(
      (result.outcome as { readonly encoded: string }).encoded,
    ) as Record<string, unknown>;
    assert.deepEqual(encoded.signed, { $i64: "-9223372036854775808" });
    assert.deepEqual(encoded.unsigned, { $u64: "18446744073709551615" });
    assert.deepEqual(encoded.decimal, {
      $decimal: { coefficient_twos_complement: "ew==", scale: 2, precision: 3 },
    });
    assert.deepEqual(encoded.money, {
      $money: {
        currency: "USD",
        amount: { coefficient_twos_complement: "ew==", scale: 2, precision: 3 },
      },
    });
    assert.deepEqual(encoded.bytes, { $bytes: "c2FmZQ==" });
    assert.deepEqual(encoded.date, { $date: 1 });
    assert.deepEqual(encoded.timestamp, {
      $timestamp: { seconds: "-1", nanos: 999_999_999 },
    });
    assert.deepEqual(encoded.embedding, { $vector: [0, 1.5, -2.25, 0.5] });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("named queries reject a returned plan outside the generated exact identity", async () => {
  const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-query-identity-"));
  const executable = join(directory, "riffdb-fake.cjs");
  await writeFile(executable, `#!/usr/bin/env node
process.stdout.write(JSON.stringify({
  schema: "riffdb.cli.output/v1",
  ok: true,
  result: {
    identity: {
      contract_lineage: "SafeApplication",
      contract_version: "1",
      contract_bundle_hash: "${"b".repeat(64)}",
      module_hash: "${"c".repeat(64)}",
      query_name: "ItemPage",
      plan_hash: "${"e".repeat(64)}",
    },
    outcome: "Found",
    application_head: "1",
    fields: [],
    next_cursor: null,
  },
}) + "\\n");
`);
  await chmod(executable, 0o700);
  try {
    const transport = new CliApplicationTransport({
      riffdbPath: executable,
      endpoint: "http://127.0.0.1:7443",
      credentialFile: join(directory, "credential"),
    });
    await assert.rejects(
      transport.executeNamedQuery({
        contractLineage: "SafeApplication",
        contractVersion: 1,
        contractBundleHash: "b".repeat(64),
        moduleHash: "c".repeat(64),
        queryName: "ItemPage",
        planHash: "d".repeat(64),
        parameters: {},
        parameterSchema: { kind: "record", fields: [] },
        resultSchemas: { Found: { kind: "record", fields: [] } },
        decodeError: () => new Error("unexpected application error"),
      }),
      /RiffDB application identity mismatch/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("named queries preserve exact money results and reject currency drift", async () => {
  const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-query-money-"));
  const executable = join(directory, "riffdb-fake.cjs");
  await writeFile(executable, `#!/usr/bin/env node
process.stdout.write(JSON.stringify({
  schema: "riffdb.cli.output/v1",
  ok: true,
  result: {
    identity: {
      contract_lineage: "Commerce",
      contract_version: "1",
      contract_bundle_hash: "${"b".repeat(64)}",
      module_hash: "${"c".repeat(64)}",
      query_name: "ProductPage",
      plan_hash: "${"d".repeat(64)}",
    },
    outcome: "Found",
    application_head: "1",
    fields: [{
      name: "product",
      cardinality: "one",
      records: [{ fields: [
        { name: "product_id", value: { type: "uuid", value: "01900000-0000-7000-8000-000000000001" } },
        { name: "price", value: { type: "money", currency: "USD", amount: {
          coefficient_twos_complement: "ew==", scale: 2,
        } } },
      ] }],
    }],
    next_cursor: null,
  },
}) + "\\n");
`);
  await chmod(executable, 0o700);
  try {
    const transport = new CliApplicationTransport({
      riffdbPath: executable,
      endpoint: "http://127.0.0.1:7443",
      credentialFile: join(directory, "credential"),
    });
    const request = {
      contractLineage: "Commerce" as const,
      contractVersion: 1 as const,
      contractBundleHash: "b".repeat(64),
      moduleHash: "c".repeat(64),
      queryName: "ProductPage",
      planHash: "d".repeat(64),
      parameters: {},
      parameterSchema: { kind: "record" as const, fields: [] },
      resultSchemas: {
        Found: {
          kind: "record" as const,
          fields: [{
            name: "product",
            schema: {
              kind: "record" as const,
              fields: [
                { name: "product_id", schema: { kind: "uuid" as const } },
                { name: "price", schema: { kind: "money" as const, currency: "USD", precision: 38, scale: 2 } },
              ],
            },
          }],
        },
      },
      decodeError: () => new Error("unexpected application error"),
    };
    const result = await transport.executeNamedQuery(request);
    const product = (result.value as {
      readonly product: {
        readonly price: {
          readonly currency: string;
          readonly amount: { readonly coefficientTwosComplement: Uint8Array; readonly precision: number; readonly scale: number };
        };
      };
    }).product;
    assert.equal(product.price.currency, "USD");
    assert.deepEqual(product.price.amount, {
      coefficientTwosComplement: Uint8Array.of(123),
      precision: 38,
      scale: 2,
    });

    await assert.rejects(
      transport.executeNamedQuery({
        ...request,
        resultSchemas: {
          Found: {
            kind: "record" as const,
            fields: [{
              name: "product",
              schema: {
                kind: "record" as const,
                fields: [
                  { name: "product_id", schema: { kind: "uuid" as const } },
                  { name: "price", schema: { kind: "money" as const, currency: "EUR", precision: 38, scale: 2 } },
                ],
              },
            }],
          },
        },
      }),
      /invalid RiffDB application response/,
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("reactive consumers preserve typed parameters, delivery values, status, and lease outcomes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-reactive-"));
  const executable = join(directory, "riffdb-fake.cjs");
  await writeFile(executable, `#!/usr/bin/env node
const args = process.argv.slice(2);
const parameters = args.flatMap((value, index) => value === "--parameter" ? [args[index + 1]] : []);
const expected = [
  'count={"type":"i64","value":"9223372036854775807"}',
  'workspace_id={"type":"uuid","value":"01900000-0000-7000-8000-000000000001"}',
];
if (JSON.stringify(parameters) !== JSON.stringify(expected)) process.exit(9);
const mutation = args.includes("ack");
process.stdout.write(JSON.stringify({
  schema: "riffdb.cli.output/v1",
  ok: true,
  result: mutation ? { result: "stale_lease" } : {
    events: [{
      event_id: "7:0", event_name: "RowChanged", attempt: 1,
      lease_token: "${"a".repeat(64)}", history_incarnation: "3",
      expires_at: { seconds: "12", nanos: 4 },
      fields: [{ name: "value", value: { type: "i64", value: "42" } }],
    }],
    wait_timed_out: false,
    status: { revision: "2", checkpoint: "before-first", history_incarnation: "3", live_leases: 1, retries: 0, dead_letters: 0 },
  },
}) + "\\n");
`);
  await chmod(executable, 0o700);
  try {
    const transport = new CliApplicationTransport({
      riffdbPath: executable,
      endpoint: "http://127.0.0.1:7443",
      credentialFile: join(directory, "credential"),
    });
    const request = {
      reactiveModuleHash: "b".repeat(64),
      operationName: "RowChanges",
      parameters: {
        count: 9_223_372_036_854_775_807n,
        workspace_id: "01900000-0000-7000-8000-000000000001",
      },
      parameterSchema: {
        kind: "record" as const,
        fields: [
          { name: "count", schema: { kind: "i64" as const } },
          { name: "workspace_id", schema: { kind: "uuid" as const } },
        ],
      },
      consumerName: "Worker_1",
    };
    const iterator = transport.consumeEventStream<typeof request.parameters, { readonly type: "RowChanged"; readonly value: bigint }>(request)[Symbol.asyncIterator]();
    const batch = await iterator.next();
    assert.equal(batch.done, false);
    assert.equal(batch.value?.events[0]?.event.value, 42n);
    assert.equal(batch.value?.status.historyIncarnation, 3n);
    const delivery = batch.value?.events[0];
    assert.ok(delivery);
    assert.equal(await transport.acknowledgeEvent(request, delivery), "stale_lease");
    await iterator.return?.();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("contextual transport preserves work evidence, hydration, reactions, and command identity", async () => {
  const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-contextual-"));
  const executable = join(directory, "riffdb-fake.cjs");
  await writeFile(executable, `#!/usr/bin/env node
const fs = require("node:fs");
const args = process.argv.slice(2);
let result;
if (args.includes("next")) {
  result = {
    items: [{
      delivery: {
        event_id: "7:0", event_name: "TicketCreated", attempt: 1,
        lease_token: "${"a".repeat(64)}", history_incarnation: "3",
        expires_at: { seconds: "12", nanos: 4 },
        fields: [{ name: "priority", value: { type: "i64", value: "42" } }],
      },
      context_head: "9",
      hydrations: [{
        name: "ticket", outcome: "Found",
        fields: [{ name: "ticket", cardinality: 1, rows: [{
          entity: "Ticket", fields: [{ name: "title", value: { type: "string", value: "checked" } }],
        }] }],
      }],
      available_reactions: [{
        name: "comment", command_name: "CreateComment", command_id: 4,
        causation_token: "${"c".repeat(66)}",
      }],
    }],
    wait_timed_out: false,
    status: { revision: "2", checkpoint: "before-first", history_incarnation: "3", live_leases: 1, retries: 0, dead_letters: 0 },
  };
} else if (args.includes("status")) {
  result = { found: true, status: { revision: "2", checkpoint: "before-first", history_incarnation: "3", live_leases: 1, retries: 0, dead_letters: 0 } };
} else if (args.includes("react")) {
  const inputPath = args[args.indexOf("--input") + 1];
  if (!inputPath || !fs.readFileSync(inputPath, "utf8").includes("checked")) process.exit(9);
  result = {
    plan_hash: "${"d".repeat(64)}", outcome_type: "CommentCreated",
    outcome: { type: "record", fields: [{ field_id: 1, value: { type: "string", value: "done" } }] },
    contract_version: "1", commit_sequence: "10", status: "committed",
  };
} else {
  result = { result: "applied" };
}
process.stdout.write(JSON.stringify({ schema: "riffdb.cli.output/v1", ok: true, result }) + "\\n");
`);
  await chmod(executable, 0o700);
  try {
    const transport = new CliApplicationTransport({
      riffdbPath: executable,
      endpoint: "http://127.0.0.1:7443",
      credentialFile: join(directory, "credential"),
    });
    const request = {
      reactiveModuleHash: "b".repeat(64),
      operationName: "TriageTicket",
      parameters: { organization_id: "01900000-0000-7000-8000-000000000001" },
      parameterSchema: {
        kind: "record" as const,
        fields: [{ name: "organization_id", schema: { kind: "uuid" as const } }],
      },
      consumerName: "TriageWorker",
    };
    const batch = await transport.consumeContextualSubscription<
      typeof request.parameters,
      { readonly type: "TicketCreated"; readonly priority: bigint }
    >(request, 1_000);
    const item = batch.items[0];
    assert.ok(item);
    assert.equal(item.delivery.event.priority, 42n);
    assert.equal(item.contextHead, 9n);
    assert.deepEqual(item.hydrations[0]?.fields.ticket, { title: "checked" });
    const reaction = item.availableReactions[0];
    assert.ok(reaction);
    assert.equal(await transport.acknowledgeContextualItem(request, item), "applied");
    assert.equal(await transport.negativeAcknowledgeContextualItem(request, item, 25), "applied");
    assert.equal((await transport.contextualSubscriptionStatus(request))?.historyIncarnation, 3n);
    const outcome = await transport.executeContextualReaction(request, reaction, {
      contractLineage: "TicketDesk",
      contractVersion: 1,
      commandName: "CreateComment",
      planHash: "d".repeat(64),
      input: { body: "checked" },
      idempotencyKey: "ignored-by-contextual-service",
      inputSchema: { kind: "record", fields: [{ name: "body", schema: { kind: "string" } }] },
      outcomeSchemas: {
        CommentCreated: {
          kind: "record",
          fields: [{ name: "status", schema: { kind: "string" }, wireId: 1 }],
        },
      },
      decodeError: () => new Error("unexpected application error"),
    });
    assert.equal(outcome.commitSequence, 10n);
    assert.deepEqual(outcome.outcome, { outcome: "CommentCreated", status: "done" });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
