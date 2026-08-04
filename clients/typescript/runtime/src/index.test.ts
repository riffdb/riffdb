import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { CliApplicationTransport } from "./index.js";

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
      },
      idempotencyKey: "scalar-command",
      inputSchema: {
        kind: "record",
        fields: [
          { name: "signed", schema: { kind: "i64" } },
          { name: "unsigned", schema: { kind: "u64" } },
          { name: "decimal", schema: { kind: "decimal" } },
          { name: "money", schema: { kind: "money" } },
          { name: "bytes", schema: { kind: "bytes" } },
          { name: "date", schema: { kind: "date" } },
          { name: "timestamp", schema: { kind: "timestamp" } },
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
          coefficient_twos_complement: "ew==", precision: 38, scale: 2,
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
          readonly amount: { readonly coefficientTwosComplement: Uint8Array; readonly precision?: number; readonly scale: number };
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
