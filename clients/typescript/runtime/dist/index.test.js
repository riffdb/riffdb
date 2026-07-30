import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { CliApplicationTransport } from "./index.js";
test("transport configuration is bounded before any process is started", () => {
    assert.throws(() => new CliApplicationTransport({ riffdbPath: "", endpoint: "http://127.0.0.1:1", credentialFile: "credential" }), /invalid RiffDB application transport configuration/);
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
        const encoded = JSON.parse(result.outcome.encoded);
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
    }
    finally {
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
        await assert.rejects(transport.executeNamedQuery({
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
        }), /RiffDB application identity mismatch/);
    }
    finally {
        await rm(directory, { recursive: true, force: true });
    }
});
