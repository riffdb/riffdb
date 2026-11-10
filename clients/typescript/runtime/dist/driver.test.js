// req: DRV-008, DRV-012
import assert from "node:assert/strict";
import { mkdir, readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { DRIVER_ERROR_REGISTRY_HASH, DRIVER_PROTOCOL_VERSION, DRIVER_VALUE_REGISTRY_HASH, DriverApplicationError, DriverApplicationTransport, exactDecimal, exactMoney, } from "./driver.js";
import { DriverGeneratedApplicationTransport, InputBudgetError } from "./index.js";
const HASH = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
let fixtureSequence = 0;
const identity = {
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
        const input = { ticket_id: { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c998" } };
        const [first, second] = await Promise.all([
            transport.invoke(operation, input), transport.invoke(operation, input),
        ]);
        assert.equal(first.applicationHead, 7n);
        assert.equal(second.applicationHead, 7n);
        assert.equal(fixture.connections, 1);
        assert.equal(fixture.handshakes, 1);
        assert.equal(fixture.invocations, 2);
        await transport.shutdown();
    }
    finally {
        await fixture.close();
    }
});
test("driver transport carries a canonical half-mebibyte byte value within its frame", async () => {
    const fixture = await DriverFixture.start((request) => request.type === "invoke" ? {
        type: "result", request_id: request.request_id,
        value: { type: "record", value: { outcome: { type: "enum", value: "Written" } } },
        application_head: 1, cursor: null, replayed: false,
    } : undefined);
    try {
        const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
        const context = Buffer.alloc(524_288, 0xa5).toString("base64");
        await transport.invoke({ name: "policy_write_mutations", inputSchemaHash: HASH }, { context: { type: "bytes", value: context } });
        assert.match(fixture.lastRequest, /"type":"bytes"/u);
        await transport.shutdown();
    }
    finally {
        await fixture.close();
    }
});
test("generated driver transport rejects aggregate canonical bytes before invocation", async () => {
    const fixture = await DriverFixture.start(() => undefined);
    let driver;
    try {
        driver = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
        const transport = new DriverGeneratedApplicationTransport(driver);
        await assert.rejects(transport.executeCommand({
            driverOperation: { name: "policy_write_mutations", inputSchemaHash: HASH },
            contractLineage: "Policy",
            contractVersion: 1,
            commandName: "WritePolicyMutations",
            planHash: HASH,
            input: { contexts: [new Uint8Array(450_000), new Uint8Array(450_000)] },
            idempotencyKey: "request-1",
            inputSchema: {
                kind: "record",
                fields: [{
                        name: "contexts",
                        schema: {
                            kind: "list", minimum: 1, maximum: 100,
                            aggregateCanonicalElementBytes: 900_000,
                            value: { kind: "bytes" },
                        },
                    }],
            },
            outcomeSchemas: { Written: { kind: "record", fields: [] } },
            decodeError: () => new Error("application error"),
        }, 3), (error) => {
            assert.ok(error instanceof InputBudgetError);
            assert.equal(error.cause, "aggregate_canonical_element_bytes");
            assert.deepEqual(error.path, { collection: "contexts" });
            return true;
        });
        assert.equal(fixture.invocations, 0);
    }
    finally {
        await driver?.shutdown();
        await fixture.close();
    }
});
test("an empty-database query preserves application frontier zero", async () => {
    const fixture = await DriverFixture.start((request) => request.type === "invoke" ? {
        type: "result", request_id: request.request_id,
        value: { type: "record", value: { outcome: { type: "enum", value: "NotFound" } } },
        application_head: 0, cursor: null, replayed: false,
    } : undefined);
    try {
        const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
        const result = await transport.invoke({ name: "better_auth_find_user_by_email", inputSchemaHash: HASH }, { email: { type: "string", value: "first@example.test" } });
        assert.equal(result.applicationHead, 0n);
        await transport.shutdown();
    }
    finally {
        await fixture.close();
    }
});
test("compact query results preserve compiler order and reject malformed rows", async () => {
    const fixture = await DriverFixture.start((request) => request.type === "invoke" ? {
        type: "compact_query_result", request_id: request.request_id,
        outcome: "Found", result_name: "tickets", entity: "Ticket",
        fields: ["ticket_id", "project_id", "title"],
        rows: [[
                { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c998" },
                { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c999" },
                { type: "string", value: "Cannot sign in" },
            ]],
        application_head: 17, cursor: "rfcur_17",
    } : undefined);
    const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
    try {
        const result = await transport.invoke({ name: "ticketdesk_board_page450", inputSchemaHash: HASH }, {}, { acceptCompactResult: true });
        assert.deepEqual(result.compact?.fields, ["ticket_id", "project_id", "title"]);
        assert.equal(result.compact?.rows.length, 1);
        assert.equal(result.applicationHead, 17n);
        assert.equal(result.cursor, "rfcur_17");
        assert.match(fixture.lastRequest, /"accept_compact_result":true/u);
    }
    finally {
        await transport.shutdown();
        await fixture.close();
    }
    for (const response of [
        { fields: ["ticket_id", "ticket_id"], rows: [[{ type: "null" }, { type: "null" }]] },
        { fields: ["ticket_id", "title"], rows: [[{ type: "null" }]] },
    ]) {
        const malformed = await DriverFixture.start((request) => request.type === "invoke" ? {
            type: "compact_query_result", request_id: request.request_id,
            outcome: "Found", result_name: "tickets", entity: "Ticket",
            application_head: 17, cursor: null, ...response,
        } : undefined);
        const client = await DriverApplicationTransport.connect({ socketPath: malformed.path, identity });
        try {
            await assert.rejects(client.invoke({ name: "ticketdesk_board_page450", inputSchemaHash: HASH }, {}, { acceptCompactResult: true }), /invalid compact result/u);
        }
        finally {
            await client.shutdown();
            await malformed.close();
        }
    }
});
test("packed query results preserve compiler columns and require closed negotiation", async () => {
    const first = Buffer.from([1, 6, 0, 0, 0, 1, 0x61]);
    const second = Buffer.from([1, 6, 0, 0, 0, 2, 0x62, 0x63]);
    const data = Buffer.concat([first, second]);
    const fixture = await DriverFixture.start((request) => request.type === "invoke" ? {
        type: "packed_query_result", request_id: request.request_id,
        outcome: "Found", result_name: "tickets", entity: "Ticket",
        fields: ["title"], row_count: 2,
        columns: [{ data: data.toString("base64"), offsets: [0, first.length, data.length] }],
        application_head: 19, cursor: "rfcur_19",
    } : undefined);
    const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
    try {
        await assert.rejects(transport.invoke({ name: "ticketdesk_board_page450", inputSchemaHash: HASH }, {}, { acceptPackedResult: true }), /invalid RiffDB driver invocation options/u);
        const result = await transport.invoke({ name: "ticketdesk_board_page450", inputSchemaHash: HASH }, {}, { acceptCompactResult: true, acceptPackedResult: true });
        assert.equal(result.packed?.rowCount, 2);
        assert.deepEqual(result.packed?.fields, ["title"]);
        assert.deepEqual(Array.from(result.packed?.columns[0]?.data ?? []), Array.from(data));
        assert.equal(result.applicationHead, 19n);
        assert.equal(result.cursor, "rfcur_19");
        assert.match(fixture.lastRequest, /"accept_packed_result":true/u);
    }
    finally {
        await transport.shutdown();
        await fixture.close();
    }
});
test("concurrent sessions use distinct driver request identities", async () => {
    const requestIds = new Set();
    const fixture = await DriverFixture.start((request) => {
        if (request.type !== "invoke")
            return undefined;
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
    }
    finally {
        await fixture.close();
    }
});
test("u64 frontiers remain exact across the JSON number protocol", async () => {
    const maximum = 18446744073709551615n;
    const golden = (await readFile(new URL("../../../../fixtures/driver/v1/max-u64-result.json", import.meta.url), "utf8")).trim();
    const fixture = await DriverFixture.start((request, socket) => {
        if (request.type !== "invoke")
            return undefined;
        writeRawFrame(socket, golden.replace('"max-u64"', JSON.stringify(request.request_id)));
        return undefined;
    });
    try {
        const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
        const result = await transport.invoke({ name: "ticketdesk_after_commit", inputSchemaHash: HASH }, {}, { readAfterCommit: maximum, queryConsistency: "admissionHead" });
        assert.equal(result.applicationHead, maximum);
        assert.match(fixture.lastRequest, /"read_after_commit":18446744073709551615/);
        assert.doesNotMatch(fixture.lastRequest, /"read_after_commit":"18446744073709551615"/);
        assert.match(fixture.lastRequest, /"query_consistency":"admission_head"/);
        await transport.shutdown();
    }
    finally {
        await fixture.close();
    }
});
test("query frontiers reject values outside the closed non-negative u64 domain", async (context) => {
    for (const [name, applicationHead] of [
        ["negative number", -1],
        ["fractional number", 1.5],
        ["negative string", "-1"],
        ["non-canonical zero", "00"],
        ["malformed string", "not-a-u64"],
        ["above u64", "18446744073709551616"],
    ]) {
        await context.test(name, async () => {
            const fixture = await DriverFixture.start((request) => request.type === "invoke" ? {
                type: "result", request_id: request.request_id, value: { type: "null" },
                application_head: applicationHead, cursor: null, replayed: false,
            } : undefined);
            const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
            try {
                await assert.rejects(transport.invoke({ name: "ticketdesk_invalid_frontier", inputSchemaHash: HASH }, {}), /invalid (?:RiffDB driver message|message)/u);
            }
            finally {
                await transport.shutdown();
                await fixture.close();
            }
        });
    }
});
test("positive-only driver identities continue to reject zero", async (context) => {
    await context.test("batch commit sequence", async () => {
        const fixture = await DriverFixture.start((request) => request.type === "batch" ? {
            type: "batch_result", request_id: request.request_id, checkpoint: 1, total: 1,
            items: [{
                    index: 0,
                    outcome: {
                        type: "result", value: { type: "null" }, commit_sequence: 0,
                        outcome_uri: "riffdb://outcomes/create/zero", replayed: false,
                    },
                }],
        } : undefined);
        const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
        try {
            await assert.rejects(transport.batch({ name: "ticketdesk_create", inputSchemaHash: HASH }, [{}], 1, 0), /invalid RiffDB driver message/u);
        }
        finally {
            await transport.shutdown();
            await fixture.close();
        }
    });
    await context.test("error contract version", async () => {
        const fixture = await DriverFixture.start((request) => request.type === "invoke" ? {
            type: "error", request_id: request.request_id, code: "RDB-APP-0001", category: "application",
            operation: "ticketdesk_find", symbol_path: [], contract_lineage: "TicketDesk", contract_version: 0,
            trace_id: null, incident_id: null, message: "safe error", retryability: "not_retryable",
            recovery_action: "none", outcome_uncertain: false,
        } : undefined);
        const transport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
        try {
            await assert.rejects(transport.invoke({ name: "ticketdesk_find", inputSchemaHash: HASH }, {}), /invalid RiffDB driver message/u);
        }
        finally {
            await transport.shutdown();
            await fixture.close();
        }
    });
    await context.test("handshake contract version", async () => {
        const fixture = await DriverFixture.start(() => undefined, { contract_version: 0 });
        try {
            await assert.rejects(DriverApplicationTransport.connect({ socketPath: fixture.path, identity }), /invalid RiffDB driver message/u);
        }
        finally {
            await fixture.close();
        }
    });
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
        const result = await transport.batch({ name: "ticketdesk_create_comment", inputSchemaHash: HASH }, [{ body: { type: "string", value: "hello" } }], 1, 0);
        assert.equal(result.items[0]?.result?.commitSequence, 9n);
        assert.equal(result.items[0]?.result?.outcomeUri, "riffdb://outcomes/create/one");
        assert.equal(result.items[0]?.result?.replayed, true);
        await transport.shutdown();
    }
    finally {
        await fixture.close();
    }
});
test("generated facade adapter owns value assembly and typed page decoding", async () => {
    const fixture = await DriverFixture.start((request) => {
        if (request.type !== "invoke")
            return undefined;
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
    }
    finally {
        await fixture.close();
    }
});
test("generated reactive facade retains cursors and lease evidence on one session", async () => {
    let watchCalls = 0;
    const fixture = await DriverFixture.start((request) => {
        if (request.type !== "invoke")
            return undefined;
        if (request.operation === "ticket_events_next") {
            const input = request.input;
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
            const input = request.input;
            assert.deepEqual(input.event_id, { type: "string", value: "12:0" });
            assert.deepEqual(input.history_incarnation, { type: "u64", value: "2" });
            return { type: "result", request_id: request.request_id, value: { type: "enum", value: "applied" }, application_head: null, cursor: null, replayed: false };
        }
        if (request.operation === "ticket_events_nack") {
            const input = request.input;
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
            const options = request.options;
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
            parameterSchema: { kind: "record", fields: [{ name: "organization_id", schema: { kind: "uuid" } }] },
            consumerName: "ticket-worker",
        };
        const controller = new AbortController();
        const iterator = transport.consumeEventStream(request, { batchLimit: 4, inFlightLimit: 8, leaseSeconds: 90, maximumWaitMs: 1, signal: controller.signal })[Symbol.asyncIterator]();
        const first = await iterator.next();
        assert.equal(first.value?.events[0]?.event.title, "Cannot sign in");
        const delivery = first.value?.events[0];
        assert.ok(delivery !== undefined);
        assert.equal(await transport.acknowledgeEvent(request, delivery), "applied");
        assert.equal(await transport.negativeAcknowledgeEvent(request, delivery, 25), "applied");
        controller.abort();
        assert.equal((await iterator.next()).done, true);
        const watch = transport.watchNamedQuery({
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
    }
    finally {
        await fixture.close();
    }
});
test("abort propagates to the exact request and preserves command uncertainty", async () => {
    let blockedRequest;
    let blockedSocket;
    const fixture = await DriverFixture.start((request, socket) => {
        if (request.type === "invoke") {
            blockedRequest = request;
            blockedSocket = socket;
            return undefined;
        }
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
        const result = transport.invoke({ name: "ticketdesk_create_comment", inputSchemaHash: HASH }, { body: { type: "string", value: "hello" } }, { signal: controller.signal });
        controller.abort();
        await assert.rejects(result, (error) => {
            assert.ok(error instanceof DriverApplicationError);
            assert.equal(error.details.code, "RDB-APP-0002");
            assert.equal(error.details.outcomeUncertain, true);
            assert.equal(error.details.recoveryAction, "resolve_with_same_idempotency_key");
            return true;
        });
        assert.equal(fixture.cancellations, 1);
        await transport.shutdown();
    }
    finally {
        await fixture.close();
    }
});
test("configuration has no remote endpoint or credential surface", async () => {
    await assert.rejects(DriverApplicationTransport.connect({ socketPath: "relative.sock", identity }), /socket path/);
    const source = await import("node:fs/promises").then(({ readFile }) => readFile(new URL("./driver.js", import.meta.url), "utf8"));
    for (const forbidden of ["child_process", "credentialFile", "riffdbPath", "grpc", "Authorization: Bearer"]) {
        assert.equal(source.includes(forbidden), false, `runtime contains ${forbidden}`);
    }
});
class DriverFixture {
    path;
    root;
    server;
    connections = 0;
    handshakes = 0;
    invocations = 0;
    cancellations = 0;
    lastRequest = "";
    sockets = new Set();
    constructor(path, root, server) {
        this.path = path;
        this.root = root;
        this.server = server;
    }
    static async start(handler, handshakeOverrides = {}) {
        fixtureSequence += 1;
        const root = join(homedir(), "tmp", `riffdb-ts-driver-${process.pid}-${fixtureSequence}`);
        await mkdir(root, { recursive: true, mode: 0o700 });
        const path = join(root, "driver.sock");
        let fixture;
        const server = createServer((socket) => {
            fixture.connections += 1;
            fixture.sockets.add(socket);
            socket.on("close", () => fixture.sockets.delete(socket));
            let input = Buffer.alloc(0);
            socket.on("data", (chunk) => {
                input = Buffer.concat([input, chunk]);
                while (input.length >= 4) {
                    const length = input.readUInt32BE(0);
                    if (input.length < length + 4)
                        return;
                    fixture.lastRequest = input.subarray(4, length + 4).toString("utf8");
                    const request = JSON.parse(fixture.lastRequest);
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
                            ...handshakeOverrides,
                        });
                        continue;
                    }
                    if (request.type === "invoke")
                        fixture.invocations += 1;
                    if (request.type === "cancel")
                        fixture.cancellations += 1;
                    const response = handler(request, socket);
                    if (response !== undefined)
                        writeFrame(socket, response);
                }
            });
        });
        fixture = new DriverFixture(path, root, server);
        await new Promise((resolve, reject) => {
            server.once("error", reject);
            server.listen(path, () => resolve());
        });
        return fixture;
    }
    async close() {
        for (const socket of this.sockets) {
            socket.destroy();
        }
        await new Promise((resolve) => this.server.close(() => resolve()));
        await rm(this.root, { recursive: true, force: true });
    }
}
function writeFrame(socket, value) {
    writeRawFrame(socket, JSON.stringify(value));
}
function writeRawFrame(socket, value) {
    const body = Buffer.from(value, "utf8");
    const frame = Buffer.alloc(body.length + 4);
    frame.writeUInt32BE(body.length);
    body.copy(frame, 4);
    socket.write(frame);
}
function driverStatus() {
    return { type: "record", value: {
            revision: { type: "u64", value: "1" },
            checkpoint: { type: "string", value: "12:0" },
            history_incarnation: { type: "u64", value: "2" },
            live_leases: { type: "u64", value: "1" },
            retries: { type: "u64", value: "0" },
            dead_letters: { type: "u64", value: "0" },
        } };
}
// req: DRV-005, DRV-007, DRV-008, DX-004, DX-046
test("driver transport exposes bound tenant from options or RIFFDB_TENANT", async () => {
    const fixture = await DriverFixture.start(() => undefined);
    const previousTenant = process.env.RIFFDB_TENANT;
    try {
        process.env.RIFFDB_TENANT = "tenant-from-env";
        const envTransport = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
        const generated = new DriverGeneratedApplicationTransport(envTransport);
        assert.equal(envTransport.tenant, "tenant-from-env");
        assert.equal(generated.tenant, "tenant-from-env");
        await envTransport.shutdown();
        const explicitTransport = await DriverApplicationTransport.connect({
            socketPath: fixture.path,
            identity,
            tenant: "tenant-explicit",
        });
        assert.equal(explicitTransport.tenant, "tenant-explicit");
        await explicitTransport.shutdown();
        await assert.rejects(DriverApplicationTransport.connect({
            socketPath: fixture.path,
            identity,
            tenant: "invalid tenant with spaces",
        }), /invalid RiffDB tenant identifier/u);
    }
    finally {
        if (previousTenant === undefined)
            delete process.env.RIFFDB_TENANT;
        else
            process.env.RIFFDB_TENANT = previousTenant;
        await fixture.close();
    }
});
// req: DRV-005, DRV-008, DX-004
test("driver transport handshakes with minimal contract identity and serializes decoded records to JSON", async () => {
    const fixture = await DriverFixture.start((request) => {
        if (request.type !== "invoke")
            return undefined;
        const input = request.input;
        assert.deepEqual(input.expected_revision, { type: "u64", value: "3" });
        assert.deepEqual(input.priority, { type: "i64", value: "2" });
        assert.deepEqual(input.due_at, {
            type: "timestamp",
            value: { seconds: "1711886400", nanos: 500_000_000 },
        });
        return {
            type: "result",
            request_id: request.request_id,
            value: {
                type: "record",
                value: {
                    outcome: { type: "enum", value: "Found" },
                    todo: {
                        type: "record",
                        value: {
                            todo_id: { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c999" },
                            revision: { type: "u64", value: "4" },
                            priority: { type: "i64", value: "2" },
                            created_at: { type: "timestamp", value: { seconds: "1711886400", nanos: 0 } },
                        },
                    },
                },
            },
            application_head: 12,
            cursor: null,
            replayed: false,
        };
    });
    const previousSocket = process.env.RIFFDB_DRIVER_SOCKET;
    const previousRole = process.env.RIFFDB_ROLE;
    try {
        process.env.RIFFDB_DRIVER_SOCKET = fixture.path;
        process.env.RIFFDB_ROLE = "TicketDeskApplication";
        const driver = await DriverApplicationTransport.connectFromEnv({
            identity: {
                contractLineage: "TicketDesk",
                contractVersion: 5n,
                contractBundleHash: HASH,
            },
        });
        assert.equal(driver.identity.database, "default");
        assert.equal(driver.identity.applicationManifestHash, HASH);
        assert.equal(driver.identity.operationCatalogHash, HASH);
        const transport = new DriverGeneratedApplicationTransport(driver);
        const result = await transport.executeNamedQuery({
            driverOperation: { name: "ticketdesk_get_todo", inputSchemaHash: HASH },
            contractLineage: "TicketDesk",
            contractVersion: 5,
            contractBundleHash: HASH,
            moduleHash: HASH,
            queryName: "GetTodo",
            planHash: HASH,
            parameters: {
                expected_revision: "3",
                priority: 2,
                due_at: "2024-03-31T12:00:00.500Z",
            },
            parameterSchema: {
                kind: "record",
                fields: [
                    { name: "expected_revision", schema: { kind: "u64" } },
                    { name: "priority", schema: { kind: "i64" } },
                    { name: "due_at", schema: { kind: "timestamp" } },
                ],
            },
            resultSchemas: {
                Found: {
                    kind: "record",
                    fields: [
                        {
                            name: "todo",
                            schema: {
                                kind: "record",
                                fields: [
                                    { name: "todo_id", schema: { kind: "uuid" } },
                                    { name: "revision", schema: { kind: "u64" } },
                                    { name: "priority", schema: { kind: "i64" } },
                                    { name: "created_at", schema: { kind: "timestamp" } },
                                ],
                            },
                        },
                    ],
                },
            },
            decodeError: () => new Error("unexpected"),
        });
        const value = result.value;
        assert.equal(value.todo.revision, 4n);
        assert.equal(value.todo.priority, 2n);
        assert.equal(value.todo.created_at.toISOString(), "2024-03-31T12:00:00.000Z");
        assert.equal(value.todo.created_at.toDate().getTime(), 1711886400000);
        assert.deepEqual(JSON.parse(JSON.stringify(value)), {
            outcome: "Found",
            todo: {
                todo_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c999",
                revision: "4",
                priority: 2,
                created_at: "2024-03-31T12:00:00.000Z",
            },
        });
        await driver.shutdown();
        delete process.env.RIFFDB_DRIVER_SOCKET;
        delete process.env.RIFFDB_ROLE;
        process.env.AGENT_ALPHA_RIFFDB_DRIVER_SOCKET = fixture.path;
        process.env.AGENT_ALPHA_RIFFDB_ROLE = "TicketDeskApplication";
        const prefixedDriver = await DriverApplicationTransport.connectFromEnv({
            prefix: "AGENT_ALPHA_",
            identity: {
                contractLineage: "TicketDesk",
                contractVersion: 5n,
                contractBundleHash: HASH,
            },
        });
        assert.equal(prefixedDriver.identity.role, "TicketDeskApplication");
        await prefixedDriver.shutdown();
        process.env.RIFFDB_DRIVER_SOCKET = fixture.path;
        process.env.RIFFDB_ROLE = "TicketDeskApplication";
        await assert.rejects(DriverApplicationTransport.connectFromEnv({
            prefix: "MISSING_APP_",
            identity: {
                contractLineage: "TicketDesk",
                contractVersion: 5n,
                contractBundleHash: HASH,
            },
        }), /invalid RiffDB driver socket path/u);
    }
    finally {
        delete process.env.AGENT_ALPHA_RIFFDB_DRIVER_SOCKET;
        delete process.env.AGENT_ALPHA_RIFFDB_ROLE;
        if (previousSocket === undefined)
            delete process.env.RIFFDB_DRIVER_SOCKET;
        else
            process.env.RIFFDB_DRIVER_SOCKET = previousSocket;
        if (previousRole === undefined)
            delete process.env.RIFFDB_ROLE;
        else
            process.env.RIFFDB_ROLE = previousRole;
        await fixture.close();
    }
});
// req: DRV-008, DRV-012, DX-012, DX-050, ID-001
test("generated TypeScript client normalizes batch idempotency keys, updates causal readAfterCommit on native batches, and deduplicates query All sets", async () => {
    const root = join(homedir(), "tmp", `riffdb-ts-generated-client-${process.pid}`);
    await mkdir(root, { recursive: true, mode: 0o700 });
    let fixture;
    try {
        const { execFile } = await import("node:child_process");
        const { promisify } = await import("node:util");
        const { pathToFileURL } = await import("node:url");
        const execFileAsync = promisify(execFile);
        const tscPath = new URL("../node_modules/typescript/bin/tsc", import.meta.url).pathname;
        const agentAlphaClientTs = new URL("../../../../examples/agent-alpha/web/src/generated/client.ts", import.meta.url).pathname;
        const corpusClientTs = new URL("../../../../fixtures/riffql/operational-access-corpus-v1/generated/typescript/client.ts", import.meta.url).pathname;
        await execFileAsync(process.execPath, [
            tscPath,
            "--ignoreConfig",
            "--target", "ES2022",
            "--module", "NodeNext",
            "--moduleResolution", "NodeNext",
            "--strict",
            "--outDir", root,
            agentAlphaClientTs,
            corpusClientTs,
        ]);
        const agentAlphaMod = await import(pathToFileURL(join(root, "examples/agent-alpha/web/src/generated/client.js")).href);
        fixture = await DriverFixture.start((request) => {
            if (request.type === "batch") {
                const inputs = request.items;
                assert.equal(inputs.length, 2);
                assert.equal(inputs[0]?.idempotency_key?.type, "string");
                assert.ok((inputs[0]?.idempotency_key?.value.length ?? 0) > 0);
                assert.equal(inputs[1]?.idempotency_key?.type, "string");
                assert.ok((inputs[1]?.idempotency_key?.value.length ?? 0) > 0);
                assert.notEqual(inputs[0]?.idempotency_key?.value, inputs[1]?.idempotency_key?.value);
                return {
                    type: "batch_result",
                    request_id: request.request_id,
                    checkpoint: 2,
                    total: 2,
                    items: [
                        {
                            index: 0,
                            outcome: {
                                type: "result",
                                value: {
                                    type: "record",
                                    value: {
                                        outcome: { type: "enum", value: "ItemExists" },
                                        item_id: { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c998" },
                                    },
                                },
                                commit_sequence: 42,
                                outcome_uri: "riffdb://outcomes/create/0",
                                replayed: false,
                            },
                        },
                        {
                            index: 1,
                            outcome: {
                                type: "result",
                                value: {
                                    type: "record",
                                    value: {
                                        outcome: { type: "enum", value: "ItemExists" },
                                        item_id: { type: "uuid", value: "018f0f79-7b5e-7c03-9b12-b16f57a4c999" },
                                    },
                                },
                                commit_sequence: 43,
                                outcome_uri: "riffdb://outcomes/create/1",
                                replayed: false,
                            },
                        },
                    ],
                };
            }
            if (request.type === "invoke") {
                const options = request.options;
                assert.equal(options.read_after_commit, 43);
                return {
                    type: "result",
                    request_id: request.request_id,
                    value: {
                        type: "record",
                        value: {
                            outcome: { type: "enum", value: "NotFound" },
                        },
                    },
                    application_head: 43,
                    cursor: null,
                    replayed: false,
                };
            }
            return undefined;
        }, {
            contract_lineage: agentAlphaMod.CONTRACT_LINEAGE,
            contract_version: agentAlphaMod.CONTRACT_VERSION,
            contract_bundle_hash: agentAlphaMod.CONTRACT_BUNDLE_HASH,
        });
        const driver = await DriverApplicationTransport.connect({
            socketPath: fixture.path,
            identity: {
                contractLineage: agentAlphaMod.CONTRACT_LINEAGE,
                contractVersion: BigInt(agentAlphaMod.CONTRACT_VERSION),
                contractBundleHash: agentAlphaMod.CONTRACT_BUNDLE_HASH,
                role: "TicketDeskApplication",
            },
        });
        const transport = new DriverGeneratedApplicationTransport(driver);
        const client = agentAlphaMod.AgentAlphaClient.fromTransport(transport);
        const batchResult = await client.createItemBatch([
            { item_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c998", title: "First" },
            { item_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c999", title: "Second" },
        ], { concurrency: 2 });
        assert.equal(batchResult.checkpoint, 2);
        assert.equal(batchResult.items.length, 2);
        assert.equal(client.lastCommitSequence, 43n);
        const page = await client.itemPage({ item_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c998" });
        assert.equal(page.value.outcome, "NotFound");
        await driver.shutdown();
        // Passing DriverGeneratedApplicationTransport or DriverApplicationTransport class constructor directly to connect / connectFromEnv
        const classConnected = await agentAlphaMod.AgentAlphaClient.connect(DriverGeneratedApplicationTransport, { socketPath: fixture.path });
        assert.ok(classConnected);
        const envConnected = await agentAlphaMod.AgentAlphaClient.connectFromEnv(DriverGeneratedApplicationTransport, { env: { RIFFDB_DRIVER_SOCKET: fixture.path } });
        assert.ok(envConnected);
        const adapterClassConnected = await agentAlphaMod.AgentAlphaClient.connect(DriverApplicationTransport, { socketPath: fixture.path });
        assert.ok(adapterClassConnected);
        const corpusMod = await import(pathToFileURL(join(root, "fixtures/riffql/operational-access-corpus-v1/generated/typescript/client.js")).href);
        let queryCalls = 0;
        const corpusClient = corpusMod.OperationalAccessCorpusClient.fromTransport({
            async executeNamedQuery(request) {
                queryCalls += 1;
                assert.deepEqual(request.parameters.codes, ["CODE-1"]);
                return {
                    identity: {
                        contractLineage: corpusMod.CONTRACT_LINEAGE,
                        contractVersion: corpusMod.CONTRACT_VERSION,
                        contractBundleHash: corpusMod.CONTRACT_BUNDLE_HASH,
                        moduleHash: corpusMod.QUERY_MODULE_HASH,
                        queryName: request.queryName,
                        planHash: corpusMod.ITEMS_BY_CODES_QUERY_PLAN_HASH,
                    },
                    value: {
                        outcome: "Found",
                        items: [{
                                item_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c998",
                                code: "CODE-1",
                                kind: "alpha",
                                score: 10n,
                                published_at: null,
                            }],
                    },
                    applicationHead: 50n,
                };
            },
            async executeCommand() {
                throw new Error("unexpected command");
            },
        });
        const duplicateCodes = Array.from({ length: 201 }, () => "CODE-1");
        const allResult = await corpusClient.itemsByCodesAll({
            workspace_id: "018f0f79-7b5e-7c03-9b12-b16f57a4c998",
            codes: duplicateCodes,
        });
        assert.equal(queryCalls, 1);
        assert.equal(allResult.value.items.length, 1);
        assert.equal(allResult.value.items[0]?.code, "CODE-1");
    }
    finally {
        await fixture?.close();
        await rm(root, { recursive: true, force: true });
    }
});
// req: DRV-008, DRV-012, DX-014, QRY-001
test("driver reactive adapter decodes vector values, protected consumer status, and empty-database live snapshots, and rejects incomplete batch receipts", async () => {
    const float = new Float32Array([0.25, -0.5, 1.5]);
    const componentBits = Array.from(new Uint32Array(float.buffer));
    const fixture = await DriverFixture.start((request) => {
        if (request.type === "batch") {
            return {
                type: "batch_result",
                request_id: request.request_id,
                checkpoint: 2,
                total: 2,
                items: [],
            };
        }
        if (request.type !== "invoke")
            return undefined;
        if (request.operation === "vector_events_next") {
            return {
                type: "result",
                request_id: request.request_id,
                value: {
                    type: "record",
                    value: {
                        events: {
                            type: "list",
                            value: [{
                                    type: "record",
                                    value: {
                                        event_id: { type: "string", value: "1:0" },
                                        event_name: { type: "enum", value: "EmbeddingUpdated" },
                                        history_incarnation: { type: "u64", value: "3" },
                                        fields: {
                                            type: "record",
                                            value: {
                                                embedding: { type: "vector", value: { component_bits: componentBits } },
                                            },
                                        },
                                        attempt: { type: "u64", value: "1" },
                                        lease_token: { type: "string", value: "bGVhc2U=" },
                                        expires_at: { type: "timestamp", value: { seconds: "99", nanos: 0 } },
                                    },
                                }],
                        },
                        wait_timed_out: { type: "bool", value: false },
                        status: {
                            type: "record",
                            value: {
                                kind: { type: "enum", value: "protected" },
                                history_incarnation: { type: "u64", value: "3" },
                                progress_cursor: { type: "bytes", value: "cHJvZ3Jlc3M=" },
                            },
                        },
                    },
                },
                application_head: null,
                cursor: null,
                replayed: false,
            };
        }
        if (request.operation === "empty_watch") {
            return {
                type: "result",
                request_id: request.request_id,
                value: {
                    type: "record",
                    value: {
                        kind: { type: "enum", value: "snapshot" },
                        result: { type: "record", value: { outcome: { type: "enum", value: "Found" } } },
                        checkpoint: {
                            type: "record",
                            value: {
                                history_incarnation: { type: "u64", value: "1" },
                                application_head: { type: "u64", value: "0" },
                                cursor: { type: "string", value: "cursor-0" },
                            },
                        },
                    },
                },
                application_head: 0,
                cursor: "cursor-0",
                replayed: false,
            };
        }
        return undefined;
    });
    try {
        const driver = await DriverApplicationTransport.connect({ socketPath: fixture.path, identity });
        const transport = new DriverGeneratedApplicationTransport(driver);
        await assert.rejects(driver.batch({ name: "ticketdesk_create_comment", inputSchemaHash: HASH }, [
            { body: { type: "string", value: "first" } },
            { body: { type: "string", value: "second" } },
        ], 2, 0), /invalid batch result/u);
        const controller = new AbortController();
        const stream = transport.consumeEventStream({
            driverOperations: {
                next: { name: "vector_events_next", inputSchemaHash: HASH },
            },
            reactiveModuleHash: HASH,
            operationName: "VectorEvents",
            parameters: {},
            parameterSchema: { kind: "record", fields: [] },
            consumerName: "vector-worker",
        }, { signal: controller.signal })[Symbol.asyncIterator]();
        const batch = await stream.next();
        controller.abort();
        assert.deepEqual(batch.value?.events[0]?.event.embedding, [0.25, -0.5, 1.5]);
        assert.equal(batch.value?.status.kind, "protected");
        assert.equal(batch.value?.status.progressCursor, "cHJvZ3Jlc3M=");
        assert.equal(batch.value?.status.historyIncarnation, 3n);
        const watchController = new AbortController();
        const watch = transport.watchNamedQuery({
            driverOperations: { watch: { name: "empty_watch", inputSchemaHash: HASH } },
            reactiveModuleHash: HASH,
            operationName: "EmptyWatch",
            parameters: {},
            parameterSchema: { kind: "record", fields: [] },
            signal: watchController.signal,
        })[Symbol.asyncIterator]();
        const snapshot = await watch.next();
        watchController.abort();
        assert.equal(snapshot.value?.type, "snapshot");
        if (snapshot.value?.type === "snapshot") {
            assert.equal(snapshot.value.applicationHead, 0n);
            assert.equal(snapshot.value.cursor, "cursor-0");
        }
        await driver.shutdown();
    }
    finally {
        await fixture.close();
    }
});
test("driver transport automatically reconnects and re-handshakes when peer restarts", async () => {
    let dropFirstInvoke = true;
    const fixture = await DriverFixture.start((request, socket) => {
        if (request.type === "invoke") {
            if (dropFirstInvoke) {
                dropFirstInvoke = false;
                socket.destroy();
                return undefined;
            }
            return {
                type: "result",
                request_id: request.request_id,
                value: { type: "record", value: { outcome: { type: "enum", value: "Found" } } },
                application_head: 99,
                cursor: null,
                replayed: false,
            };
        }
        return undefined;
    });
    try {
        const transport = await DriverApplicationTransport.connectFromEnv({
            env: { RIFFDB_DRIVER_SOCKET: fixture.path },
        });
        assert.equal(transport.identity.contractLineage, "TicketDesk");
        assert.equal(transport.identity.role, "TicketDeskApplication");
        const op = { name: "ticketdesk_get_ticket", inputSchemaHash: HASH };
        await assert.rejects(transport.invoke(op, {}), /RiffDB driver session (?:closed|failed)/u);
        const recovered = await transport.invoke(op, {});
        assert.equal(recovered.applicationHead, 99n);
        assert.equal(fixture.handshakes, 2);
        await transport.shutdown();
    }
    finally {
        await fixture.close();
    }
});
