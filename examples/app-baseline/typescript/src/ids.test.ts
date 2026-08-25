import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { formatUuid, uuidFromOrdinal } from "./ids.js";
import { INTERACTIVE_WEIGHTS, LoadReport, OpStats } from "./load.js";
import { OBLIGATIONS, SCHEMA_SQL, safeInputFingerprint } from "./schema.js";
import { boardDenseOpenCount, fullScale, generateSeed, smokeScale } from "./seed.js";

const repoRoot = fileURLToPath(new URL("../../../../", import.meta.url));

test("uuid_from_ordinal matches rust layout", () => {
  const value = uuidFromOrdinal(0x10, 0);
  assert.equal(value.length, 16);
  assert.equal(value[0], 0x10);
  assert.equal(value[6], 0x70);
  assert.equal(value[8]! & 0xc0, 0x80);
  assert.equal(formatUuid(value), "10101010-1010-7010-8000-000000000000");
});

test("fingerprint is canonical and length prefixed", () => {
  const first = safeInputFingerprint([Buffer.from("a/b"), Buffer.from("c")]);
  const second = safeInputFingerprint([Buffer.from("a"), Buffer.from("b/c")]);
  const repeated = safeInputFingerprint([Buffer.from("a/b"), Buffer.from("c")]);
  assert.equal(first.length, 64);
  assert.equal(first, repeated);
  assert.notEqual(first, second);
  assert.equal(first.includes("a/b"), false);
});

test("smoke and full seed shapes", () => {
  const smoke = generateSeed(smokeScale());
  assert.equal(smoke.organizations.length, 2);
  assert.equal(smoke.scale.boardDenseOpen, 0);
  assert.ok(boardDenseOpenCount(smoke) < 50);
  const full = generateSeed(fullScale());
  assert.equal(boardDenseOpenCount(full), 600);
});

test("schema contains safe-app obligations", () => {
  for (const table of [
    "app_permission",
    "app_idempotency",
    "app_audit",
    "app_domain_event",
    "app_outbox_intent",
  ]) {
    assert.ok(SCHEMA_SQL.includes(`CREATE TABLE ${table}`));
  }
  assert.deepEqual(OBLIGATIONS, [
    "symbolic_operation_authorization",
    "idempotency_admission_and_equal_input_replay",
    "domain_mutation",
    "audit_and_provenance",
    "domain_event",
    "outbox_intent",
    "one_atomic_transaction",
  ]);
});

test("python schema tables match this schema", () => {
  const rust = readFileSync(`${repoRoot}examples/app-baseline/postgres/src/lib.rs`, "utf8");
  const rustTables = new Set([...rust.matchAll(/CREATE TABLE (\w+)/g)].map((match) => match[1]));
  const typescriptTables = new Set(
    [...SCHEMA_SQL.matchAll(/CREATE TABLE (\w+)/g)].map((match) => match[1]),
  );
  assert.deepEqual(typescriptTables, rustTables);
});

test("interactive weights are mostly reads", () => {
  const total = INTERACTIVE_WEIGHTS.reduce((sum, [, weight]) => sum + weight, 0);
  const writes = INTERACTIVE_WEIGHTS.filter(([name]) =>
    ["create_comment", "close_ticket_with_comment", "open_ticket_with_labels"].includes(name),
  ).reduce((sum, [, weight]) => sum + weight, 0);
  assert.ok(total - writes > writes);
  assert.equal(
    INTERACTIVE_WEIGHTS.some(([name]) => name === "swap_member_roles"),
    false,
  );
});

test("riffdb driver contains no typescript safety implementation", () => {
  const source = readFileSync(new URL("../src/riffdb.ts", import.meta.url), "utf8");
  for (const forbidden of [
    "app_permission",
    "app_idempotency",
    "app_audit",
    "app_domain_event",
    "app_outbox_intent",
    "PERMISSION_SQL",
    "IDEMPOTENCY_LOCK_SQL",
    "INSERT_AUDIT_SQL",
    "INSERT_EVENT_SQL",
    "INSERT_OUTBOX_SQL",
    "safeAdmit",
    "safeComplete",
    "safeInputFingerprint",
  ]) {
    assert.equal(source.includes(forbidden), false, forbidden);
  }
  assert.ok(source.includes("No TypeScript safety code"));
  assert.ok(source.includes("generated TicketDesk"));
});

test("postgres driver owns the sql safety tables", () => {
  const source = readFileSync(new URL("../src/postgres.ts", import.meta.url), "utf8");
  assert.ok(source.includes("safeAdmit"));
  assert.ok(source.includes("safeComplete"));
  assert.ok(source.includes("INSERT_OUTBOX_SQL"));
});

test("generated ticketdesk client is present", () => {
  const text = readFileSync(`${repoRoot}clients/typescript/ticketdesk/client.ts`, "utf8");
  assert.ok(text.includes("export class TicketDeskClient"));
  assert.ok(text.includes("createComment"));
});

test("report records which side owns safety", () => {
  const riff = new LoadReport(
    "riffdb_public_grpc",
    1,
    "interactive",
    1_000_000_000,
    1,
    new OpStats(),
    {},
    [0],
  ).json();
  const postgres = new LoadReport(
    "postgres_safe_app",
    1,
    "interactive",
    1_000_000_000,
    1,
    new OpStats(),
    {},
    [0],
  ).json();
  assert.equal(riff.safety_owner, "riffdbd_rust");
  assert.equal(postgres.safety_owner, "typescript_sql");
  assert.equal(riff.evidentiary, false);
  assert.ok(
    (riff.notes as string[]).join(" ").includes(
      "No authorization, idempotency, audit, event, or outbox code runs in TypeScript",
    ),
  );
});
