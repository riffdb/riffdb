import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
const executeFile = promisify(execFile);
const MAX_OUTPUT_BYTES = 4_194_304;
const HASH = /^[0-9a-f]{64}$/;
const SYMBOL = /^[A-Za-z][A-Za-z0-9_.-]{0,255}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
/**
 * Constructs an exact fixed-point value from decimal text without using a
 * JavaScript floating-point number.
 */
export function exactDecimal(value, precision, scale) {
    if (!Number.isInteger(precision) || precision < 1 || precision > 38
        || !Number.isInteger(scale) || scale < 0 || scale > precision) {
        throw new Error("invalid exact decimal type");
    }
    if (typeof value !== "string" || value.length < 1 || value.length > 128) {
        throw new Error("invalid exact decimal text");
    }
    const match = /^(-?)([0-9]+)(?:\.([0-9]+))?$/.exec(value);
    if (match === null)
        throw new Error("invalid exact decimal text");
    const fractional = match[3] ?? "";
    if (fractional.length > scale)
        throw new Error("exact decimal exceeds scale");
    const magnitudeText = `${match[2]}${fractional.padEnd(scale, "0")}`;
    let coefficient = BigInt(magnitudeText);
    if (match[1] === "-" && coefficient !== 0n)
        coefficient = -coefficient;
    const limit = 10n ** BigInt(precision);
    if (coefficient <= -limit || coefficient >= limit)
        throw new Error("exact decimal exceeds precision");
    return {
        coefficientTwosComplement: signedTwosComplement(coefficient),
        scale,
        precision,
    };
}
/** Constructs the exact precision-38, scale-2 value used by `money<CURRENCY>`. */
export function exactMoney(currency, value) {
    if (!/^[A-Z]{3}$/.test(currency))
        throw new Error("invalid exact money currency");
    return { currency, amount: exactDecimal(value, 38, 2) };
}
export class CliApplicationTransport {
    options;
    constructor(options) {
        this.options = options;
        if (options.riffdbPath.length === 0 || options.riffdbPath.length > 4096
            || options.endpoint.length === 0 || options.endpoint.length > 2048
            || options.credentialFile.length === 0 || options.credentialFile.length > 4096) {
            throw new Error("invalid RiffDB application transport configuration");
        }
    }
    async executeNamedQuery(request, options = {}) {
        validateIdentity(request);
        const parameters = encodeValue(request.parameters, request.parameterSchema);
        const args = this.baseArguments();
        args.push("query", "run-named", request.queryName, "--module-hash", request.moduleHash, "--contract-lineage", request.contractLineage, "--contract-version", String(request.contractVersion));
        if (options.cursor !== undefined)
            args.push("--cursor", options.cursor);
        if (options.readAfterCommit !== undefined) {
            if (options.readAfterCommit < 1n)
                throw new Error("invalid read-after-commit fence");
            args.push("--read-after-commit", options.readAfterCommit.toString());
        }
        const envelope = await this.invoke(args, parameters, request.decodeError);
        const result = exactObject(envelope.result);
        const rawIdentity = exactObject(result.identity);
        const outcome = expectSymbol(result.outcome);
        const resultSchema = request.resultSchemas[outcome];
        if (resultSchema === undefined)
            throw new Error("RiffDB application response has an unknown outcome");
        const value = decodePlain({ outcome, ...normalizeQueryFields(result.fields) }, {
            kind: "record",
            fields: [
                { name: "outcome", schema: { kind: "string" } },
                ...recordFields(resultSchema),
            ],
        });
        const typed = {
            identity: {
                contractLineage: expectSymbol(rawIdentity.contract_lineage),
                contractVersion: positiveNumber(rawIdentity.contract_version),
                contractBundleHash: expectHash(rawIdentity.contract_bundle_hash),
                moduleHash: expectHash(rawIdentity.module_hash),
                queryName: expectSymbol(rawIdentity.query_name),
                planHash: expectHash(rawIdentity.plan_hash),
            },
            value,
            applicationHead: positiveBigInt(result.application_head),
        };
        if (typed.identity.contractLineage !== request.contractLineage
            || typed.identity.contractVersion !== request.contractVersion
            || typed.identity.contractBundleHash !== request.contractBundleHash
            || typed.identity.moduleHash !== request.moduleHash
            || typed.identity.queryName !== request.queryName
            || typed.identity.planHash !== request.planHash) {
            throw new Error("RiffDB application identity mismatch");
        }
        if (result.next_cursor !== null && result.next_cursor !== undefined) {
            return { ...typed, nextCursor: expectBoundedString(result.next_cursor, 4096) };
        }
        return typed;
    }
    async executeCommand(request, attemptBudget) {
        validateIdentity(request);
        if (!Number.isInteger(attemptBudget) || attemptBudget < 1 || attemptBudget > 10) {
            throw new Error("invalid command attempt budget");
        }
        const input = encodeValue(request.input, request.inputSchema);
        const args = this.baseArguments(attemptBudget);
        args.push("command", "run", request.commandName, "--expected-version", String(request.contractVersion));
        const envelope = await this.invoke(args, input, request.decodeError);
        return decodeTypedCommandResult(exactObject(envelope.result), request);
    }
    async *consumeEventStream(request, options = {}) {
        validateReactiveRequest(request);
        const batchLimit = boundedInteger(options.batchLimit ?? 1, 1, 64);
        const inFlightLimit = boundedInteger(options.inFlightLimit ?? 16, 1, 64);
        const leaseSeconds = boundedInteger(options.leaseSeconds ?? 60, 5, 900);
        const maximumWaitMs = boundedInteger(options.maximumWaitMs ?? 30_000, 0, 30_000);
        while (true) {
            const args = this.reactiveArguments(request);
            args.push("event", "consume", "--module-hash", request.reactiveModuleHash, "--operation", request.operationName, "--consumer-name", request.consumerName, "--batch-limit", String(batchLimit), "--in-flight-limit", String(inFlightLimit), "--lease-seconds", String(leaseSeconds), "--wait-nanos", String(maximumWaitMs * 1_000_000));
            addReactiveParameters(args, request.parameters, request.parameterSchema);
            const envelope = await this.invokeArguments(args);
            const result = exactObject(envelope.result);
            const status = decodeReactiveConsumerStatus(result.status);
            const historyIncarnation = status.historyIncarnation;
            const events = expectArray(result.events).map((value) => {
                const delivery = exactObject(value);
                const fields = Object.fromEntries(expectArray(delivery.fields).map((field) => {
                    const item = exactObject(field);
                    return [expectSymbol(item.name), decodeTagged(item.value)];
                }));
                const expiration = exactObject(delivery.expires_at);
                return {
                    eventId: expectBoundedString(delivery.event_id, 64),
                    event: { type: expectSymbol(delivery.event_name), ...fields },
                    attempt: positiveNumber(delivery.attempt),
                    leaseToken: expectHash(delivery.lease_token),
                    expiresAt: `${expectBoundedString(expiration.seconds, 32)}.${String(boundedInteger(expiration.nanos, 0, 999_999_999)).padStart(9, "0")}`,
                    historyIncarnation,
                };
            });
            yield { events, waitTimedOut: result.wait_timed_out === true, status };
        }
    }
    async acknowledgeEvent(request, delivery) {
        return this.mutateEventLease("ack", request, delivery);
    }
    async negativeAcknowledgeEvent(request, delivery, retryDelayMs = 0) {
        return this.mutateEventLease("nack", request, delivery, retryDelayMs);
    }
    async seekEventConsumer(request, checkpoint) {
        validateReactiveRequest(request);
        const args = this.reactiveArguments(request);
        args.push("event", "seek", "--module-hash", request.reactiveModuleHash, "--operation", request.operationName, "--consumer-name", request.consumerName, "--checkpoint", expectBoundedString(checkpoint, 64));
        addReactiveParameters(args, request.parameters, request.parameterSchema);
        return decodeEventMutationResult((await this.invokeArguments(args)).result);
    }
    async eventConsumerStatus(request) {
        validateReactiveRequest(request);
        const args = this.reactiveArguments(request);
        args.push("event", "status", "--module-hash", request.reactiveModuleHash, "--operation", request.operationName, "--consumer-name", request.consumerName);
        addReactiveParameters(args, request.parameters, request.parameterSchema);
        const result = exactObject((await this.invokeArguments(args)).result);
        if (result.found !== true)
            return undefined;
        return decodeReactiveConsumerStatus(result.status);
    }
    async consumeContextualSubscription(request, maximumWaitMs = 30_000) {
        validateReactiveRequest(request);
        const wait = boundedInteger(maximumWaitMs, 0, 30_000);
        const args = this.reactiveArguments(request);
        args.push("contextual", "next", "--module-hash", request.reactiveModuleHash, "--operation", request.operationName, "--consumer-name", request.consumerName, "--wait-nanos", String(wait * 1_000_000));
        addReactiveParameters(args, request.parameters, request.parameterSchema);
        const result = exactObject((await this.invokeArguments(args)).result);
        return {
            items: expectArray(result.items).map((value) => decodeContextualItem(value)),
            waitTimedOut: result.wait_timed_out === true,
            status: decodeReactiveConsumerStatus(result.status),
        };
    }
    async acknowledgeContextualItem(request, item) {
        return this.mutateContextualItem("ack", request, item);
    }
    async negativeAcknowledgeContextualItem(request, item, retryDelayMs = 0) {
        return this.mutateContextualItem("nack", request, item, retryDelayMs);
    }
    async contextualSubscriptionStatus(request) {
        validateReactiveRequest(request);
        const args = this.reactiveArguments(request);
        args.push("contextual", "status", "--module-hash", request.reactiveModuleHash, "--operation", request.operationName, "--consumer-name", request.consumerName);
        addReactiveParameters(args, request.parameters, request.parameterSchema);
        const result = exactObject((await this.invokeArguments(args)).result);
        if (result.found !== true)
            return undefined;
        return decodeReactiveConsumerStatus(result.status);
    }
    async executeContextualReaction(request, reaction, command) {
        validateReactiveRequest(request);
        validateIdentity(command);
        if (reaction.commandName !== command.commandName) {
            throw new Error("contextual reaction command identity mismatch");
        }
        const input = encodeValue(command.input, command.inputSchema);
        const args = this.reactiveArguments(request);
        args.push("contextual", "react", "--module-hash", request.reactiveModuleHash, "--operation", request.operationName, "--consumer-name", request.consumerName, "--reaction", expectSymbol(reaction.name), "--causation-token", expectBoundedLowerHex(reaction.causationToken, 2_048), "--command-name", command.commandName, "--expected-version", String(command.contractVersion));
        addReactiveParameters(args, request.parameters, request.parameterSchema);
        const envelope = await this.invoke(args, input, command.decodeError, "input");
        return decodeTypedCommandResult(exactObject(envelope.result), command);
    }
    async *watchNamedQuery(request) {
        if (!HASH.test(request.reactiveModuleHash) || !SYMBOL.test(request.operationName)) {
            throw new Error("invalid reactive operation identity");
        }
        let cursor = request.cursor;
        while (true) {
            const args = this.baseArguments();
            args.push("query", "watch", request.operationName, "--module-hash", request.reactiveModuleHash);
            addReactiveParameters(args, request.parameters, request.parameterSchema);
            if (cursor !== undefined)
                args.push("--cursor", cursor);
            const result = exactObject((await this.invokeArguments(args)).result);
            const type = expectSymbol(result.type);
            if (type === "terminal") {
                yield {
                    type,
                    reason: expectSymbol(result.reason),
                    ...liveTerminalFrontier(result.last_frontier),
                };
                return;
            }
            cursor = expectBoundedString(result.cursor, 16_384);
            const frontier = exactObject(result.frontier);
            const applicationHead = nonnegativeBigInt(frontier.application_head);
            const historyIncarnation = positiveBigInt(frontier.history_incarnation);
            if (type === "snapshot") {
                yield { type, value: decodeLiveValue(result.result), cursor, applicationHead, historyIncarnation };
            }
            else if (type === "reset") {
                yield { type, reason: expectSymbol(result.reason), value: decodeLiveValue(result.result), cursor, applicationHead, historyIncarnation };
            }
            else if (type === "patch") {
                yield { type, resultField: expectSymbol(result.result_field), operations: expectArray(result.operations).map(decodeLivePatchOperation), cursor, applicationHead, historyIncarnation };
            }
            else if (type === "checkpoint") {
                yield { type, cursor, applicationHead, historyIncarnation };
            }
            else {
                throw new Error("invalid RiffDB live query update");
            }
        }
    }
    async mutateEventLease(action, request, delivery, retryDelayMs = 0) {
        validateReactiveRequest(request);
        const delay = boundedInteger(retryDelayMs, 0, 3_600_000);
        const args = this.reactiveArguments(request);
        args.push("event", action, "--module-hash", request.reactiveModuleHash, "--operation", request.operationName, "--consumer-name", request.consumerName, "--event-id", expectBoundedString(delivery.eventId, 64), "--lease-token", expectHash(delivery.leaseToken), "--history-incarnation", delivery.historyIncarnation.toString());
        if (action === "nack")
            args.push("--retry-delay-nanos", String(delay * 1_000_000));
        addReactiveParameters(args, request.parameters, request.parameterSchema);
        return decodeEventMutationResult((await this.invokeArguments(args)).result);
    }
    async mutateContextualItem(action, request, item, retryDelayMs = 0) {
        validateReactiveRequest(request);
        const delay = boundedInteger(retryDelayMs, 0, 3_600_000);
        const args = this.reactiveArguments(request);
        args.push("contextual", action, "--module-hash", request.reactiveModuleHash, "--operation", request.operationName, "--consumer-name", request.consumerName, "--event-id", expectBoundedString(item.delivery.eventId, 64), "--lease-token", expectHash(item.delivery.leaseToken), "--history-incarnation", item.delivery.historyIncarnation.toString());
        if (action === "nack")
            args.push("--retry-delay-nanos", String(delay * 1_000_000));
        addReactiveParameters(args, request.parameters, request.parameterSchema);
        return decodeEventMutationResult((await this.invokeArguments(args)).result);
    }
    reactiveArguments(request) {
        validateReactiveRequest(request);
        return this.baseArguments();
    }
    baseArguments(attemptBudget) {
        const args = [
            "--endpoint", this.options.endpoint,
            "--output", "json",
            "--credential-file", this.options.credentialFile,
        ];
        if (attemptBudget !== undefined)
            args.push("--max-attempts", String(attemptBudget));
        return args;
    }
    async invoke(args, input, decodeError, inputKind) {
        const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-"));
        const inputPath = join(directory, "input.json");
        try {
            await writeFile(inputPath, `${JSON.stringify(input)}\n`, { encoding: "utf8", mode: 0o600 });
            const commandArgs = [...args];
            const selectedInput = inputKind ?? (commandArgs.includes("command") ? "input" : "parameters");
            commandArgs.push(selectedInput === "input" ? "--input" : "--parameters", inputPath);
            let stdout;
            try {
                ({ stdout } = await executeFile(this.options.riffdbPath, commandArgs, {
                    encoding: "utf8",
                    maxBuffer: MAX_OUTPUT_BYTES,
                    timeout: 35_000,
                }));
            }
            catch (error) {
                const candidate = exactObject(error);
                stdout = typeof candidate.stdout === "string" ? candidate.stdout : "";
            }
            const envelope = exactObject(JSON.parse(stdout));
            if (envelope.schema !== "riffdb.cli.output/v1")
                throw new Error("invalid RiffDB CLI envelope");
            if (envelope.ok !== true) {
                const raw = exactObject(envelope.error);
                if (raw.type === "application")
                    throw decodeError(normalizeError(raw));
                throw new Error("RiffDB application transport failed");
            }
            return envelope;
        }
        finally {
            await rm(directory, { recursive: true, force: true });
        }
    }
    async invokeArguments(args) {
        let stdout;
        try {
            ({ stdout } = await executeFile(this.options.riffdbPath, args, {
                encoding: "utf8", maxBuffer: MAX_OUTPUT_BYTES, timeout: 35_000,
            }));
        }
        catch (error) {
            const candidate = exactObject(error);
            stdout = typeof candidate.stdout === "string" ? candidate.stdout : "";
        }
        const envelope = exactObject(JSON.parse(stdout));
        if (envelope.schema !== "riffdb.cli.output/v1")
            throw new Error("invalid RiffDB CLI envelope");
        if (envelope.ok !== true)
            throw new Error("RiffDB reactive operation failed");
        return envelope;
    }
}
/**
 * Generated-facade adapter for one retained Rust driver-host session.
 *
 * The adapter owns only checked value assembly and result decoding. Remote
 * trust, credentials, retries, pooling, and uncertainty remain in Rust.
 */
export class DriverGeneratedApplicationTransport {
    driver;
    constructor(driver) {
        this.driver = driver;
    }
    async executeNamedQuery(request, options = {}) {
        validateIdentity(request);
        const operation = requireDriverOperation(request.driverOperation);
        const result = await this.driver.invoke(operation, encodeDriverRecord(request.parameters, request.parameterSchema), {
            ...(options.cursor === undefined ? {} : { cursor: options.cursor }),
            ...(options.readAfterCommit === undefined ? {} : { readAfterCommit: options.readAfterCommit }),
        });
        if (result.applicationHead === undefined)
            throw new Error("RiffDB driver omitted the query frontier");
        const outcome = driverOutcome(result.value);
        const resultSchema = request.resultSchemas[outcome];
        if (resultSchema === undefined)
            throw new Error("RiffDB driver returned an unknown query outcome");
        const value = decodeDriverValue(result.value, {
            kind: "record",
            fields: [
                { name: "outcome", schema: { kind: "enum" } },
                ...recordFields(resultSchema),
            ],
        });
        return {
            identity: {
                contractLineage: request.contractLineage,
                contractVersion: request.contractVersion,
                contractBundleHash: request.contractBundleHash,
                moduleHash: request.moduleHash,
                queryName: request.queryName,
                planHash: request.planHash,
            },
            value,
            applicationHead: result.applicationHead,
            ...(result.cursor === undefined ? {} : { nextCursor: result.cursor }),
        };
    }
    async executeCommand(request, attemptBudget) {
        validateIdentity(request);
        const operation = requireDriverOperation(request.driverOperation);
        const result = await this.driver.invoke(operation, encodeDriverRecord(request.input, request.inputSchema), { maximumAttempts: boundedInteger(attemptBudget, 1, 10) });
        return decodeDriverCommandResult(request, result.value, result.applicationHead, result.cursor, result.replayed);
    }
    async executeCommandBatch(request, inputs, concurrency, checkpoint, attemptBudget) {
        validateIdentity(request);
        const operation = requireDriverOperation(request.driverOperation);
        const result = await this.driver.batch(operation, inputs.map((input) => encodeDriverRecord(input, request.inputSchema)), concurrency, checkpoint, { maximumAttempts: boundedInteger(attemptBudget, 1, 10) });
        return {
            checkpoint: result.checkpoint,
            items: result.items.map((item) => item.result === undefined
                ? { index: item.index, ...(item.error === undefined ? {} : { error: item.error }) }
                : {
                    index: item.index,
                    result: decodeDriverCommandResult(request, item.result.value, item.result.commitSequence, item.result.outcomeUri, item.result.replayed),
                }),
        };
    }
    async *consumeEventStream(request, options = {}) {
        validateDriverReactiveRequest(request);
        const batchLimit = boundedInteger(options.batchLimit ?? 1, 1, 64);
        const inFlightLimit = boundedInteger(options.inFlightLimit ?? 16, 1, 64);
        const leaseSeconds = boundedInteger(options.leaseSeconds ?? 60, 5, 900);
        const maximumWaitMs = boundedInteger(options.maximumWaitMs ?? 30_000, 0, 30_000);
        while (options.signal?.aborted !== true) {
            const input = reactiveDriverInput(request);
            input.batch_limit = driverU64(BigInt(batchLimit));
            input.in_flight_limit = driverU64(BigInt(inFlightLimit));
            input.lease_seconds = driverU64(BigInt(leaseSeconds));
            const result = await this.driver.invoke(requireReactiveDriverOperation(request, "next"), input, { deadlineMillis: Math.max(1, maximumWaitMs), ...(options.signal === undefined ? {} : { signal: options.signal }) });
            yield decodeDriverEventBatch(result.value);
        }
    }
    async acknowledgeEvent(request, delivery) {
        return this.mutateDriverLease("ack", request, delivery);
    }
    async negativeAcknowledgeEvent(request, delivery, retryDelayMs = 0) {
        const delay = boundedInteger(retryDelayMs, 0, 300_000);
        return this.mutateDriverLease("nack", request, delivery, BigInt(delay) * 1000000n);
    }
    async seekEventConsumer(request, checkpoint) {
        const input = reactiveDriverInput(request);
        input.checkpoint = { type: "string", value: expectBoundedString(checkpoint, 256) };
        const result = await this.driver.invoke(requireReactiveDriverOperation(request, "seek"), input);
        return decodeDriverMutationResult(result.value);
    }
    async eventConsumerStatus(request) {
        const result = await this.driver.invoke(requireReactiveDriverOperation(request, "status"), reactiveDriverInput(request));
        return result.value.type === "null" ? undefined : decodeDriverConsumerStatus(result.value);
    }
    async consumeContextualSubscription(request, maximumWaitMs = 30_000, signal) {
        const wait = boundedInteger(maximumWaitMs, 0, 30_000);
        const result = await this.driver.invoke(requireReactiveDriverOperation(request, "next"), reactiveDriverInput(request), { deadlineMillis: Math.max(1, wait), ...(signal === undefined ? {} : { signal }) });
        return decodeDriverContextualBatch(result.value);
    }
    async acknowledgeContextualItem(request, item) {
        return this.mutateDriverLease("ack", request, item.delivery);
    }
    async negativeAcknowledgeContextualItem(request, item, retryDelayMs = 0) {
        const delay = boundedInteger(retryDelayMs, 0, 300_000);
        return this.mutateDriverLease("nack", request, item.delivery, BigInt(delay) * 1000000n);
    }
    async contextualSubscriptionStatus(request) {
        return this.eventConsumerStatus(request);
    }
    async executeContextualReaction(request, reaction, command) {
        validateIdentity(command);
        if (reaction.commandName !== command.commandName) {
            throw new Error("contextual reaction command identity mismatch");
        }
        const action = `react_${snakeDriverAction(reaction.name)}`;
        const input = reactiveDriverInput(request);
        input.causation_token = { type: "string", value: expectBoundedString(reaction.causationToken, 16_384) };
        input.input = encodeDriverValue(command.input, command.inputSchema);
        const result = await this.driver.invoke(requireReactiveDriverOperation(request, action), input);
        return decodeDriverCommandResult(command, result.value, result.applicationHead, result.cursor, result.replayed);
    }
    async *watchNamedQuery(request) {
        validateDriverReactiveRequest(request);
        let cursor = request.cursor;
        while (request.signal?.aborted !== true) {
            const input = reactiveDriverInput(request);
            if (cursor !== undefined)
                input.cursor = { type: "string", value: expectBoundedString(cursor, 16_384) };
            const result = await this.driver.invoke(requireReactiveDriverOperation(request, "watch"), input, { ...(cursor === undefined ? {} : { cursor }), ...(request.signal === undefined ? {} : { signal: request.signal }) });
            const update = decodeDriverLiveUpdate(result.value, result.applicationHead, result.cursor);
            yield update;
            if (update.type === "terminal")
                return;
            cursor = update.cursor;
        }
    }
    async mutateDriverLease(action, request, delivery, retryDelayNanos) {
        const input = reactiveDriverInput(request);
        input.event_id = { type: "string", value: expectBoundedString(delivery.eventId, 256) };
        input.lease_token = { type: "string", value: expectBoundedString(delivery.leaseToken, 16_384) };
        input.history_incarnation = driverU64(delivery.historyIncarnation);
        if (action === "nack")
            input.retry_delay_nanos = driverU64(retryDelayNanos ?? 0n);
        const result = await this.driver.invoke(requireReactiveDriverOperation(request, action), input);
        return decodeDriverMutationResult(result.value);
    }
}
function validateDriverReactiveRequest(value) {
    if (!HASH.test(value.reactiveModuleHash) || !SYMBOL.test(value.operationName)) {
        throw new Error("invalid generated reactive operation identity");
    }
    if (value.parameterSchema.kind !== "record") {
        throw new Error("invalid generated reactive parameter schema");
    }
    if (value.consumerName !== undefined)
        expectSymbol(value.consumerName);
    if (value.driverOperations === undefined || Object.keys(value.driverOperations).length < 1
        || Object.keys(value.driverOperations).length > 16) {
        throw new Error("generated reactive binding has no driver operation identities");
    }
    for (const [action, operation] of Object.entries(value.driverOperations)) {
        if (!/^[a-z][a-z0-9_]{0,127}$/.test(action))
            throw new Error("invalid generated reactive action");
        requireDriverOperation(operation);
    }
}
function requireReactiveDriverOperation(request, action) {
    const operation = request.driverOperations?.[action];
    if (operation === undefined)
        throw new Error("generated reactive binding has no driver action identity");
    return requireDriverOperation(operation);
}
function reactiveDriverInput(request) {
    const input = {
        parameters: encodeDriverValue(request.parameters, request.parameterSchema),
    };
    if (request.consumerName !== undefined) {
        input.consumer_name = { type: "string", value: expectSymbol(request.consumerName) };
    }
    return input;
}
function driverU64(value) {
    if (value < 0n || value > (1n << 64n) - 1n)
        throw new Error("invalid generated RiffDB u64 value");
    return { type: "u64", value: value.toString() };
}
function decodeDriverMutationResult(value) {
    if (value.type !== "enum" || ![
        "applied", "state_changed", "not_found", "outstanding_lease", "stale_lease", "lease_expired",
    ].includes(value.value)) {
        throw new Error("invalid RiffDB driver mutation result");
    }
    return value.value;
}
function decodeDriverConsumerStatus(value) {
    const status = driverRecord(value);
    return {
        revision: driverPositiveU64(requiredDriverField(status, "revision")),
        checkpoint: driverString(requiredDriverField(status, "checkpoint"), 256),
        historyIncarnation: driverPositiveU64(requiredDriverField(status, "history_incarnation")),
        liveLeases: driverBoundedU32(requiredDriverField(status, "live_leases")),
        retries: driverBoundedU32(requiredDriverField(status, "retries")),
        deadLetters: driverBoundedU32(requiredDriverField(status, "dead_letters")),
    };
}
function decodeDriverEventBatch(value) {
    const batch = driverRecord(value);
    const events = driverList(requiredDriverField(batch, "events")).map(decodeDriverDelivery);
    const timedOut = requiredDriverField(batch, "wait_timed_out");
    if (timedOut.type !== "bool")
        throw new Error("invalid RiffDB driver event batch");
    return {
        events,
        waitTimedOut: timedOut.value,
        status: decodeDriverConsumerStatus(requiredDriverField(batch, "status")),
    };
}
function decodeDriverDelivery(value) {
    const delivery = driverRecord(value);
    const eventName = driverEnum(requiredDriverField(delivery, "event_name"));
    const fields = driverRecord(requiredDriverField(delivery, "fields"));
    const expiration = requiredDriverField(delivery, "expires_at");
    if (expiration.type !== "timestamp")
        throw new Error("invalid RiffDB driver event lease");
    return {
        eventId: driverString(requiredDriverField(delivery, "event_id"), 256),
        event: { type: eventName, ...decodeDriverDynamicRecord(fields) },
        attempt: driverBoundedU32(requiredDriverField(delivery, "attempt")),
        leaseToken: driverString(requiredDriverField(delivery, "lease_token"), 16_384),
        expiresAt: `${expiration.value.seconds}.${String(expiration.value.nanos).padStart(9, "0")}`,
        historyIncarnation: driverPositiveU64(requiredDriverField(delivery, "history_incarnation")),
    };
}
function decodeDriverContextualBatch(value) {
    const batch = driverRecord(value);
    const timedOut = requiredDriverField(batch, "wait_timed_out");
    if (timedOut.type !== "bool")
        throw new Error("invalid RiffDB driver contextual batch");
    return {
        items: driverList(requiredDriverField(batch, "items")).map((raw) => {
            const item = driverRecord(raw);
            return {
                delivery: decodeDriverDelivery(requiredDriverField(item, "delivery")),
                contextHead: driverPositiveU64(requiredDriverField(item, "context_head")),
                hydrations: driverList(requiredDriverField(item, "hydrations")).map((rawHydration) => {
                    const hydration = driverRecord(rawHydration);
                    const fields = {};
                    for (const [name, field] of Object.entries(hydration)) {
                        if (name !== "name" && name !== "outcome")
                            fields[name] = decodeDriverDynamic(field);
                    }
                    return {
                        name: driverString(requiredDriverField(hydration, "name"), 256),
                        outcome: driverEnum(requiredDriverField(hydration, "outcome")),
                        fields,
                    };
                }),
                availableReactions: driverList(requiredDriverField(item, "available_reactions")).map((rawReaction) => {
                    const reaction = driverRecord(rawReaction);
                    return {
                        name: driverString(requiredDriverField(reaction, "name"), 256),
                        commandName: driverString(requiredDriverField(reaction, "command_name"), 256),
                        commandId: driverBoundedU32(requiredDriverField(reaction, "command_id")),
                        causationToken: driverString(requiredDriverField(reaction, "causation_token"), 16_384),
                    };
                }),
            };
        }),
        waitTimedOut: timedOut.value,
        status: decodeDriverConsumerStatus(requiredDriverField(batch, "status")),
    };
}
function decodeDriverLiveUpdate(value, applicationHead, responseCursor) {
    const update = driverRecord(value);
    const kind = driverEnum(requiredDriverField(update, "kind"));
    if (kind === "terminal") {
        const reason = driverEnum(requiredDriverField(update, "reason"));
        const frontier = requiredDriverField(update, "last_frontier");
        if (frontier.type === "null")
            return { type: "terminal", reason };
        const record = driverRecord(frontier);
        return {
            type: "terminal",
            reason,
            lastApplicationHead: driverPositiveU64(requiredDriverField(record, "application_head")),
            historyIncarnation: driverPositiveU64(requiredDriverField(record, "history_incarnation")),
        };
    }
    const checkpoint = driverRecord(requiredDriverField(update, "checkpoint"));
    const cursor = driverString(requiredDriverField(checkpoint, "cursor"), 16_384);
    const head = driverPositiveU64(requiredDriverField(checkpoint, "application_head"));
    const history = driverPositiveU64(requiredDriverField(checkpoint, "history_incarnation"));
    if (applicationHead !== head || responseCursor !== cursor) {
        throw new Error("RiffDB driver live frontier is inconsistent");
    }
    if (kind === "snapshot") {
        return { type: "snapshot", value: decodeDriverDynamic(requiredDriverField(update, "result")), cursor, applicationHead: head, historyIncarnation: history };
    }
    if (kind === "reset") {
        return { type: "reset", reason: driverEnum(requiredDriverField(update, "reason")), value: decodeDriverDynamic(requiredDriverField(update, "result")), cursor, applicationHead: head, historyIncarnation: history };
    }
    if (kind === "checkpoint") {
        return { type: "checkpoint", cursor, applicationHead: head, historyIncarnation: history };
    }
    if (kind !== "patch")
        throw new Error("invalid RiffDB driver live update");
    return {
        type: "patch",
        resultField: driverString(requiredDriverField(update, "result_field"), 256),
        operations: driverList(requiredDriverField(update, "operations")).map(decodeDriverPatchOperation),
        cursor,
        applicationHead: head,
        historyIncarnation: history,
    };
}
function decodeDriverPatchOperation(value) {
    const operation = driverRecord(value);
    const kind = driverEnum(requiredDriverField(operation, "operation"));
    if (kind === "insert" || kind === "replace") {
        const record = decodeDriverDynamic(requiredDriverField(operation, "record"));
        if (!isPlainRecord(record))
            throw new Error("invalid RiffDB driver live record");
        return { type: kind, index: driverBoundedU32(requiredDriverField(operation, "index")), record };
    }
    const key = decodeDriverDynamic(requiredDriverField(operation, "key"));
    if (!isPlainRecord(key))
        throw new Error("invalid RiffDB driver live key");
    if (kind === "remove") {
        return { type: "remove", index: driverBoundedU32(requiredDriverField(operation, "index")), key };
    }
    if (kind === "move") {
        return { type: "move", from: driverBoundedU32(requiredDriverField(operation, "from")), to: driverBoundedU32(requiredDriverField(operation, "to")), key };
    }
    throw new Error("invalid RiffDB driver live patch operation");
}
function decodeDriverDynamic(value) {
    switch (value.type) {
        case "null": return null;
        case "bool": return value.value;
        case "i64":
        case "u64": return BigInt(value.value);
        case "string":
        case "uuid":
        case "enum": return value.value;
        case "bytes": return Uint8Array.from(Buffer.from(value.value, "base64"));
        case "date": return Number(value.value);
        case "timestamp": return { seconds: BigInt(value.value.seconds), nanos: value.value.nanos };
        case "decimal": {
            if (value.value.precision === undefined)
                throw new Error("invalid RiffDB driver decimal");
            return { coefficientTwosComplement: Uint8Array.from(Buffer.from(value.value.coefficient, "base64")), scale: value.value.scale, precision: value.value.precision };
        }
        case "money": {
            if (value.value.amount.precision === undefined)
                throw new Error("invalid RiffDB driver money");
            return { currency: value.value.currency, amount: { coefficientTwosComplement: Uint8Array.from(Buffer.from(value.value.amount.coefficient, "base64")), scale: value.value.amount.scale, precision: value.value.amount.precision } };
        }
        case "list": return value.value.map(decodeDriverDynamic);
        case "record": return decodeDriverDynamicRecord(value.value);
    }
}
function decodeDriverDynamicRecord(value) {
    return Object.fromEntries(Object.entries(value).map(([name, field]) => [name, decodeDriverDynamic(field)]));
}
function driverRecord(value) {
    if (value.type !== "record")
        throw new Error("invalid RiffDB driver record");
    return value.value;
}
function driverList(value) {
    if (value.type !== "list")
        throw new Error("invalid RiffDB driver list");
    return value.value;
}
function requiredDriverField(value, name) {
    const field = value[name];
    if (field === undefined)
        throw new Error("RiffDB driver result is missing a field");
    return field;
}
function driverPositiveU64(value) {
    if (value.type !== "u64")
        throw new Error("invalid RiffDB driver u64");
    const parsed = BigInt(value.value);
    if (parsed < 1n || parsed > (1n << 64n) - 1n)
        throw new Error("invalid RiffDB driver u64");
    return parsed;
}
function driverBoundedU32(value) {
    if (value.type !== "u64")
        throw new Error("invalid RiffDB driver u32");
    const parsed = BigInt(value.value);
    if (parsed < 0n)
        throw new Error("invalid RiffDB driver u32");
    if (parsed > 4294967295n)
        throw new Error("invalid RiffDB driver u32");
    return Number(parsed);
}
function driverString(value, maximum) {
    if (value.type !== "string")
        throw new Error("invalid RiffDB driver string");
    return expectBoundedString(value.value, maximum);
}
function driverEnum(value) {
    if (value.type !== "enum")
        throw new Error("invalid RiffDB driver enum");
    return expectSymbol(value.value);
}
function snakeDriverAction(value) {
    const result = value.replace(/([a-z0-9])([A-Z])/g, "$1_$2").replace(/[^A-Za-z0-9]+/g, "_").toLowerCase();
    if (!/^[a-z][a-z0-9_]{0,127}$/.test(result))
        throw new Error("invalid contextual reaction identity");
    return result;
}
function isPlainRecord(value) {
    return typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof Uint8Array);
}
function requireDriverOperation(operation) {
    if (operation === undefined)
        throw new Error("generated RiffDB binding has no driver operation identity");
    return operation;
}
function encodeDriverRecord(value, schema) {
    if (schema.kind !== "record")
        throw new Error("invalid generated RiffDB record schema");
    const encoded = encodeDriverValue(value, schema);
    if (encoded.type !== "record")
        throw new Error("invalid generated RiffDB record");
    return encoded.value;
}
function encodeDriverValue(value, schema) {
    if (schema.kind === "optional") {
        return value === null || value === undefined ? { type: "null" } : encodeDriverValue(value, schema.value);
    }
    if (schema.kind === "list") {
        if (!Array.isArray(value)
            || value.length < (schema.minimum ?? 0)
            || value.length > (schema.maximum ?? 4_096)) {
            throw new Error("invalid generated RiffDB list input");
        }
        return { type: "list", value: value.map((item) => encodeDriverValue(item, schema.value)) };
    }
    if (schema.kind === "record") {
        const input = exactObject(value);
        const fields = new Map(schema.fields.map((field) => [field.name, field.schema]));
        if (Object.keys(input).some((name) => !fields.has(name)))
            throw new Error("invalid generated RiffDB record input");
        const output = {};
        for (const [name, fieldSchema] of fields) {
            const fieldValue = input[name];
            if (fieldValue === undefined && fieldSchema.kind === "optional")
                continue;
            if (fieldValue === undefined)
                throw new Error("generated RiffDB input is missing a field");
            output[name] = encodeDriverValue(fieldValue, fieldSchema);
        }
        return { type: "record", value: output };
    }
    switch (schema.kind) {
        case "bool":
            if (typeof value !== "boolean")
                throw new Error("invalid generated RiffDB bool input");
            return { type: "bool", value };
        case "i64":
            if (typeof value !== "bigint" || value < -(1n << 63n) || value > (1n << 63n) - 1n)
                throw new Error("invalid generated RiffDB i64 input");
            return { type: "i64", value: value.toString() };
        case "u64":
            if (typeof value !== "bigint" || value < 0n || value > (1n << 64n) - 1n)
                throw new Error("invalid generated RiffDB u64 input");
            return { type: "u64", value: value.toString() };
        case "string":
        case "cursor": return { type: "string", value: expectBoundedString(value, 262_144) };
        case "uuid":
            if (typeof value !== "string" || !UUID.test(value))
                throw new Error("invalid generated RiffDB UUID input");
            return { type: "uuid", value };
        case "enum": return { type: "enum", value: expectSymbol(value) };
        case "bytes":
            if (!(value instanceof Uint8Array) || value.byteLength > 1_048_576)
                throw new Error("invalid generated RiffDB bytes input");
            return { type: "bytes", value: Buffer.from(value).toString("base64") };
        case "date":
            if (!Number.isInteger(value) || value < -2_147_483_648 || value > 2_147_483_647)
                throw new Error("invalid generated RiffDB date input");
            return { type: "date", value: String(value) };
        case "timestamp": {
            const timestamp = exactObject(value);
            if (typeof timestamp.seconds !== "bigint" || timestamp.seconds < -(1n << 63n) || timestamp.seconds > (1n << 63n) - 1n)
                throw new Error("invalid generated RiffDB timestamp input");
            return { type: "timestamp", value: { seconds: timestamp.seconds.toString(), nanos: boundedInteger(timestamp.nanos, 0, 999_999_999) } };
        }
        case "decimal": return { type: "decimal", value: encodeDriverDecimal(value, schema) };
        case "money": {
            const money = exactObject(value);
            const currency = expectCurrency(money.currency);
            if (currency !== schema.currency)
                throw new Error("generated RiffDB money currency does not match");
            return { type: "money", value: { currency, amount: encodeDriverDecimal(money.amount, schema) } };
        }
        case "limit": return { type: "u64", value: String(boundedInteger(value, 1, 500)) };
    }
}
function encodeDriverDecimal(value, schema) {
    const decimal = encodeDecimal(value);
    if (schema.precision === undefined || schema.scale === undefined || decimal.scale !== schema.scale
        || (decimal.precision !== undefined && decimal.precision !== schema.precision)) {
        throw new Error("generated RiffDB decimal does not match its schema");
    }
    return { coefficient: decimal.coefficient_twos_complement, precision: schema.precision, scale: schema.scale };
}
function driverOutcome(value) {
    if (value.type !== "record")
        throw new Error("RiffDB driver returned a non-record application result");
    const outcome = value.value.outcome;
    if (outcome?.type !== "enum")
        throw new Error("RiffDB driver returned no declared outcome");
    return expectSymbol(outcome.value);
}
function decodeDriverCommandResult(request, value, commitSequence, outcomeUri, replayed) {
    const outcome = driverOutcome(value);
    const schema = request.outcomeSchemas[outcome];
    if (schema === undefined)
        throw new Error("RiffDB driver returned an unknown command outcome");
    return {
        outcome: decodeDriverValue(value, {
            kind: "record",
            fields: [{ name: "outcome", schema: { kind: "enum" } }, ...recordFields(schema)],
        }),
        contractVersion: request.contractVersion,
        planHash: request.planHash,
        replayed,
        ...(commitSequence === undefined ? {} : { commitSequence }),
        ...(outcomeUri === undefined ? {} : { outcomeUri }),
    };
}
function decodeDriverValue(value, schema) {
    if (schema.kind === "optional")
        return value.type === "null" ? null : decodeDriverValue(value, schema.value);
    if (schema.kind === "list") {
        if (value.type !== "list"
            || value.value.length < (schema.minimum ?? 0)
            || value.value.length > (schema.maximum ?? 4_096)) {
            throw new Error("invalid RiffDB driver list result");
        }
        return value.value.map((item) => decodeDriverValue(item, schema.value));
    }
    if (schema.kind === "record") {
        if (value.type !== "record")
            throw new Error("invalid RiffDB driver record result");
        const allowed = new Set(schema.fields.map((field) => field.name));
        if (Object.keys(value.value).some((name) => !allowed.has(name)))
            throw new Error("invalid RiffDB driver record result");
        const output = {};
        for (const field of schema.fields) {
            const fieldValue = value.value[field.name];
            if (fieldValue === undefined && field.schema.kind === "optional") {
                output[field.name] = null;
                continue;
            }
            if (fieldValue === undefined)
                throw new Error("invalid RiffDB driver record result");
            output[field.name] = decodeDriverValue(fieldValue, field.schema);
        }
        return output;
    }
    switch (schema.kind) {
        case "bool":
            if (value.type === "bool")
                return value.value;
            break;
        case "i64":
            if (value.type === "i64") {
                const parsed = BigInt(value.value);
                if (parsed >= -(1n << 63n) && parsed <= (1n << 63n) - 1n)
                    return parsed;
            }
            break;
        case "u64":
            if (value.type === "u64") {
                const parsed = BigInt(value.value);
                if (parsed >= 0n && parsed <= (1n << 64n) - 1n)
                    return parsed;
            }
            break;
        case "string":
        case "cursor":
            if (value.type === "string")
                return expectBoundedString(value.value, 262_144);
            break;
        case "uuid":
            if (value.type === "uuid" && UUID.test(value.value))
                return value.value;
            break;
        case "enum":
            if (value.type === "enum")
                return expectSymbol(value.value);
            break;
        case "bytes":
            if (value.type === "bytes")
                return Uint8Array.from(Buffer.from(value.value, "base64"));
            break;
        case "date":
            if (value.type === "date") {
                const parsed = Number(value.value);
                if (Number.isInteger(parsed) && parsed >= -2_147_483_648 && parsed <= 2_147_483_647)
                    return parsed;
            }
            break;
        case "timestamp":
            if (value.type === "timestamp")
                return { seconds: BigInt(value.value.seconds), nanos: boundedInteger(value.value.nanos, 0, 999_999_999) };
            break;
        case "decimal":
            if (value.type === "decimal")
                return decodeDriverDecimal(value.value, schema);
            break;
        case "money":
            if (value.type === "money" && value.value.currency === schema.currency)
                return { currency: value.value.currency, amount: decodeDriverDecimal(value.value.amount, schema) };
            break;
        case "limit":
            if (value.type === "u64")
                return boundedInteger(Number(value.value), 1, 500);
            break;
    }
    throw new Error("RiffDB driver result does not match the generated schema");
}
function decodeDriverDecimal(value, schema) {
    if (schema.precision === undefined || schema.scale === undefined
        || (value.precision !== undefined && value.precision !== null && value.precision !== schema.precision)
        || value.scale !== schema.scale)
        throw new Error("RiffDB driver decimal does not match its schema");
    const coefficient = Uint8Array.from(Buffer.from(value.coefficient, "base64"));
    if (coefficient.byteLength < 1 || coefficient.byteLength > 16)
        throw new Error("RiffDB driver decimal coefficient is invalid");
    return { coefficientTwosComplement: coefficient, precision: schema.precision, scale: schema.scale };
}
function decodeTypedCommandResult(result, request) {
    if (expectHash(result.plan_hash) !== request.planHash) {
        throw new Error("RiffDB application identity mismatch");
    }
    const outcomeName = expectSymbol(result.outcome_type);
    const schema = request.outcomeSchemas[outcomeName];
    if (schema === undefined)
        throw new Error("RiffDB application response has an unknown outcome");
    const payload = decodeWire(result.outcome, schema);
    const outcome = { outcome: outcomeName, ...exactObject(payload) };
    const typed = {
        outcome,
        contractVersion: positiveNumber(result.contract_version),
        planHash: request.planHash,
        replayed: result.status === "replayed",
    };
    if (result.commit_sequence === undefined)
        return typed;
    return {
        ...typed,
        commitSequence: positiveBigInt(result.commit_sequence),
        ...(result.outcome_uri === undefined
            ? {}
            : { outcomeUri: expectBoundedString(result.outcome_uri, 4096) }),
    };
}
function decodeContextualItem(value) {
    const item = exactObject(value);
    return {
        delivery: decodeContextualDelivery(item.delivery),
        contextHead: positiveBigInt(item.context_head),
        hydrations: expectArray(item.hydrations).map(decodeContextualHydration),
        availableReactions: expectArray(item.available_reactions).map((raw) => {
            const reaction = exactObject(raw);
            return {
                name: expectSymbol(reaction.name),
                commandName: expectSymbol(reaction.command_name),
                commandId: positiveNumber(reaction.command_id),
                causationToken: expectBoundedLowerHex(reaction.causation_token, 2_048),
            };
        }),
    };
}
function decodeContextualDelivery(value) {
    const delivery = exactObject(value);
    const fields = Object.fromEntries(expectArray(delivery.fields).map((raw) => {
        const field = exactObject(raw);
        return [expectSymbol(field.name), decodeTagged(field.value)];
    }));
    const expiration = exactObject(delivery.expires_at);
    return {
        eventId: expectBoundedString(delivery.event_id, 64),
        event: { type: expectSymbol(delivery.event_name), ...fields },
        attempt: positiveNumber(delivery.attempt),
        leaseToken: expectHash(delivery.lease_token),
        expiresAt: `${expectBoundedString(expiration.seconds, 32)}.${String(boundedInteger(expiration.nanos, 0, 999_999_999)).padStart(9, "0")}`,
        historyIncarnation: positiveBigInt(delivery.history_incarnation),
    };
}
function decodeContextualHydration(value) {
    const hydration = exactObject(value);
    const fields = Object.fromEntries(expectArray(hydration.fields).map((raw) => {
        const field = exactObject(raw);
        const rows = expectArray(field.rows).map((rawRow) => {
            const row = exactObject(rawRow);
            expectBoundedString(row.entity, 256);
            return Object.fromEntries(expectArray(row.fields).map((rawValue) => {
                const item = exactObject(rawValue);
                return [expectSymbol(item.name), decodeTagged(item.value)];
            }));
        });
        const cardinality = boundedInteger(field.cardinality, 1, 3);
        if ((cardinality === 1 && rows.length !== 1) || (cardinality === 2 && rows.length > 1)) {
            throw new Error("invalid RiffDB contextual cardinality");
        }
        return [expectSymbol(field.name), cardinality === 3 ? rows : (rows[0] ?? null)];
    }));
    return {
        name: expectSymbol(hydration.name),
        outcome: expectSymbol(hydration.outcome),
        fields,
    };
}
function expectBoundedLowerHex(value, maximum) {
    const text = expectBoundedString(value, maximum);
    if (text.length <= 64 || text.length % 2 !== 0 || !/^[0-9a-f]+$/.test(text)) {
        throw new Error("invalid opaque RiffDB token");
    }
    return text;
}
function validateIdentity(value) {
    expectSymbol(value.contractLineage);
    positiveNumber(value.contractVersion);
    if (value.moduleHash !== undefined)
        expectHash(value.moduleHash);
    if (value.planHash !== undefined)
        expectHash(value.planHash);
    if (value.queryName !== undefined)
        expectSymbol(value.queryName);
    if (value.commandName !== undefined)
        expectSymbol(value.commandName);
}
function encodeValue(value, schema) {
    if (schema.kind === "optional") {
        return value === null || value === undefined ? null : encodeValue(value, schema.value);
    }
    if (schema.kind === "list") {
        if (!Array.isArray(value)
            || (schema.minimum !== undefined && value.length < schema.minimum)
            || (schema.maximum !== undefined && value.length > schema.maximum)) {
            throw new Error("invalid generated application input");
        }
        return value.map((item) => encodeValue(item, schema.value));
    }
    if (schema.kind === "record") {
        const input = exactObject(value);
        const allowed = new Set(schema.fields.map((field) => field.name));
        if (Object.keys(input).some((name) => !allowed.has(name))) {
            throw new Error("invalid generated application input");
        }
        const output = {};
        for (const field of schema.fields) {
            const fieldValue = input[field.name];
            if (fieldValue === undefined && field.schema.kind === "optional")
                continue;
            if (fieldValue === undefined)
                continue;
            output[field.name] = encodeValue(fieldValue, field.schema);
        }
        return output;
    }
    switch (schema.kind) {
        case "uuid":
            if (typeof value !== "string" || !UUID.test(value))
                throw new Error("invalid UUID input");
            return { $uuid: value };
        case "enum":
            return { $enum: expectSymbol(value) };
        case "string":
        case "cursor":
            return expectBoundedString(value, 262_144);
        case "bool":
            if (typeof value !== "boolean")
                throw new Error("invalid boolean input");
            return value;
        case "i64":
            if (typeof value !== "bigint" || value < -(1n << 63n) || value > (1n << 63n) - 1n) {
                throw new Error("invalid i64 input");
            }
            return { $i64: value.toString() };
        case "u64":
            if (typeof value !== "bigint" || value < 0n || value > (1n << 64n) - 1n) {
                throw new Error("invalid u64 input");
            }
            return { $u64: value.toString() };
        case "decimal": {
            const decimal = encodeDecimal(value);
            if (schema.precision === undefined || schema.scale === undefined
                || decimal.scale !== schema.scale
                || (decimal.precision !== undefined && decimal.precision !== schema.precision)) {
                throw new Error("decimal input does not match the generated type");
            }
            return { $decimal: { ...decimal, precision: schema.precision } };
        }
        case "money": {
            const input = exactObject(value);
            const currency = expectCurrency(input.currency);
            const amount = encodeDecimal(input.amount);
            if (schema.currency === undefined || schema.precision === undefined || schema.scale === undefined
                || currency !== schema.currency || amount.scale !== schema.scale
                || (amount.precision !== undefined && amount.precision !== schema.precision)) {
                throw new Error("money input does not match the generated type");
            }
            return {
                $money: {
                    currency,
                    amount: { ...amount, precision: schema.precision },
                },
            };
        }
        case "bytes":
            if (!(value instanceof Uint8Array) || value.byteLength > 1_048_576) {
                throw new Error("invalid bytes input");
            }
            return { $bytes: Buffer.from(value).toString("base64") };
        case "date":
            if (!Number.isInteger(value) || value < -2_147_483_648 || value > 2_147_483_647) {
                throw new Error("invalid date input");
            }
            return { $date: value };
        case "timestamp": {
            const input = exactObject(value);
            const seconds = input.seconds;
            const nanos = input.nanos;
            if (typeof seconds !== "bigint" || seconds < -(1n << 63n) || seconds > (1n << 63n) - 1n
                || !Number.isInteger(nanos) || nanos < 0 || nanos > 999_999_999) {
                throw new Error("invalid timestamp input");
            }
            return { $timestamp: { seconds: seconds.toString(), nanos } };
        }
        case "limit":
            if (!Number.isInteger(value) || value < 1 || value > 500) {
                throw new Error("invalid limit input");
            }
            return value;
        default:
            throw new Error("generated application scalar is not supported by the CLI transport");
    }
}
function normalizeQueryFields(value) {
    if (!Array.isArray(value) || value.length > 4096)
        throw new Error("invalid RiffDB query fields");
    const output = {};
    for (const item of value) {
        const field = exactObject(item);
        const name = expectSymbol(field.name);
        if (name in output || !Array.isArray(field.records))
            throw new Error("invalid RiffDB query fields");
        const records = field.records.map((record) => {
            const raw = exactObject(record);
            if (!Array.isArray(raw.fields))
                throw new Error("invalid RiffDB query record");
            const result = {};
            for (const item of raw.fields) {
                const entry = exactObject(item);
                const fieldName = expectSymbol(entry.name);
                if (fieldName in result)
                    throw new Error("duplicate RiffDB query field");
                result[fieldName] = decodeTagged(entry.value);
            }
            return result;
        });
        switch (field.cardinality) {
            case "one":
                if (records.length !== 1)
                    throw new Error("invalid one cardinality");
                output[name] = records[0];
                break;
            case "maybe":
                if (records.length > 1)
                    throw new Error("invalid maybe cardinality");
                output[name] = records[0] ?? null;
                break;
            case "many":
                output[name] = records;
                break;
            default:
                throw new Error("invalid RiffDB query cardinality");
        }
    }
    return output;
}
function decodeTagged(value) {
    const input = exactObject(value);
    switch (input.type) {
        case "null": return null;
        case "bool": return input.value === true ? true : input.value === false ? false : fail();
        case "i64":
        case "u64": return BigInt(expectDecimal(input.value));
        case "string":
        case "uuid": return expectBoundedString(input.value, 262_144);
        case "enum": return expectSymbol(input.name);
        case "date": return Number(expectDecimal(input.days_since_unix_epoch));
        case "timestamp": return { seconds: BigInt(expectDecimal(input.seconds)), nanos: nonnegativeNumber(input.nanos) };
        case "bytes": return Uint8Array.from(Buffer.from(expectBoundedString(input.value, MAX_OUTPUT_BYTES), "base64"));
        case "decimal": return decodeDecimal(input);
        case "money": return {
            currency: expectCurrency(input.currency),
            amount: decodeDecimal(exactObject(input.amount)),
        };
        case "list":
            if (!Array.isArray(input.values))
                return fail();
            return input.values.map(decodeTagged);
        default: return fail();
    }
}
function encodeDecimal(value) {
    const input = exactObject(value);
    const coefficient = input.coefficientTwosComplement;
    if (!(coefficient instanceof Uint8Array) || coefficient.byteLength < 1 || coefficient.byteLength > 16
        || !Number.isInteger(input.scale) || input.scale < 0
        || input.scale > 4_294_967_295) {
        throw new Error("invalid decimal input");
    }
    const output = {
        coefficient_twos_complement: Buffer.from(coefficient).toString("base64"),
        scale: input.scale,
    };
    if (input.precision !== undefined) {
        if (!Number.isInteger(input.precision) || input.precision < 1
            || input.precision > 4_294_967_295) {
            throw new Error("invalid decimal precision");
        }
        output.precision = input.precision;
    }
    return output;
}
function decodeDecimal(input) {
    const coefficient = Uint8Array.from(Buffer.from(expectBoundedString(input.coefficient_twos_complement, 24), "base64"));
    const scale = nonnegativeNumber(input.scale);
    if (coefficient.byteLength < 1 || coefficient.byteLength > 16)
        return fail();
    if (input.precision === undefined)
        return { coefficientTwosComplement: coefficient, scale };
    const precision = positiveNumber(input.precision);
    return { coefficientTwosComplement: coefficient, scale, precision };
}
function decodePlain(value, schema) {
    if (schema.kind === "optional")
        return value === null ? null : decodePlain(value, schema.value);
    if (schema.kind === "list") {
        if (!Array.isArray(value)
            || (schema.minimum !== undefined && value.length < schema.minimum)
            || (schema.maximum !== undefined && value.length > schema.maximum))
            return fail();
        return value.map((item) => decodePlain(item, schema.value));
    }
    if (schema.kind === "record") {
        const input = exactObject(value);
        const output = {};
        if (Object.keys(input).length !== schema.fields.length)
            return fail();
        for (const field of schema.fields)
            output[field.name] = decodePlain(input[field.name], field.schema);
        return output;
    }
    switch (schema.kind) {
        case "bool":
            if (typeof value === "boolean")
                return value;
            break;
        case "i64":
        case "u64":
            if (typeof value === "bigint")
                return value;
            break;
        case "string":
        case "enum":
        case "cursor":
            if (typeof value === "string")
                return value;
            break;
        case "uuid":
            if (typeof value === "string" && UUID.test(value))
                return value;
            break;
        case "date":
        case "limit":
            if (Number.isInteger(value))
                return value;
            break;
        case "timestamp": {
            const input = exactObject(value);
            if (typeof input.seconds === "bigint" && Number.isInteger(input.nanos))
                return value;
            break;
        }
        case "bytes":
            if (value instanceof Uint8Array)
                return value;
            break;
        case "decimal": {
            return normalizeDecodedDecimal(value, schema.precision, schema.scale);
        }
        case "money": {
            const input = exactObject(value);
            const currency = expectCurrency(input.currency);
            if (schema.currency !== undefined && currency !== schema.currency)
                return fail();
            return {
                currency,
                amount: normalizeDecodedDecimal(input.amount, schema.precision, schema.scale),
            };
        }
        default: break;
    }
    return fail();
}
function normalizeDecodedDecimal(value, expectedPrecision, expectedScale) {
    const input = exactObject(value);
    const encoded = encodeDecimal(value);
    if ((expectedPrecision !== undefined && encoded.precision !== undefined
        && encoded.precision !== expectedPrecision)
        || (expectedScale !== undefined && encoded.scale !== expectedScale))
        return fail();
    const precision = expectedPrecision ?? encoded.precision;
    return {
        coefficientTwosComplement: input.coefficientTwosComplement,
        scale: encoded.scale,
        ...(precision === undefined ? {} : { precision }),
    };
}
function decodeWire(value, schema) {
    if (schema.kind !== "record")
        return decodePlain(decodeTagged(value), schema);
    const input = exactObject(value);
    if (input.type !== "record" || !Array.isArray(input.fields))
        return fail();
    const fields = new Map();
    for (const item of input.fields) {
        const field = exactObject(item);
        const id = positiveNumber(field.field_id);
        if (fields.has(id))
            return fail();
        fields.set(id, field.value);
    }
    const output = {};
    for (const field of schema.fields) {
        if (field.wireId === undefined)
            return fail();
        const raw = fields.get(field.wireId);
        if (raw === undefined)
            return fail();
        output[field.name] = decodeWire(raw, field.schema);
        fields.delete(field.wireId);
    }
    if (fields.size !== 0)
        return fail();
    return output;
}
function normalizeError(input) {
    const output = {
        type: input.type,
        code: input.code,
        message: input.message,
        category: input.category,
        recoveryAction: input.recovery_action,
        operation: input.operation,
        symbolPath: input.symbol_path ?? [],
        fixes: input.fixes,
    };
    if (input.contract_lineage !== undefined)
        output.contractLineage = input.contract_lineage;
    if (input.contract_version !== undefined)
        output.contractVersion = Number(expectDecimal(input.contract_version));
    if (input.operation_symbol !== undefined)
        output.operationSymbol = input.operation_symbol;
    if (input.source_span !== undefined) {
        const span = exactObject(input.source_span);
        output.sourceSpan = { start: Number(expectDecimal(span.start)), end: Number(expectDecimal(span.end)) };
    }
    if (input.trace_id !== undefined)
        output.traceId = input.trace_id;
    if (input.incident_id !== undefined)
        output.incidentId = input.incident_id;
    return output;
}
function recordFields(schema) {
    if (schema.kind !== "record")
        return fail();
    return schema.fields;
}
function exactObject(value) {
    if (typeof value !== "object" || value === null || Array.isArray(value))
        return fail();
    return value;
}
function expectArray(value) {
    if (!Array.isArray(value) || value.length > 100_000)
        throw new Error("invalid RiffDB collection");
    return value;
}
function boundedInteger(value, minimum, maximum) {
    if (typeof value !== "number" || !Number.isInteger(value) || value < minimum || value > maximum) {
        throw new Error("invalid bounded integer");
    }
    return value;
}
function nonnegativeBigInt(value) {
    if (typeof value !== "string" || !/^(0|[1-9][0-9]{0,19})$/.test(value)) {
        throw new Error("invalid nonnegative integer");
    }
    return BigInt(value);
}
function validateReactiveRequest(request) {
    if (!HASH.test(request.reactiveModuleHash)
        || !SYMBOL.test(request.operationName)
        || !/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(request.consumerName)) {
        throw new Error("invalid reactive consumer identity");
    }
}
function addReactiveParameters(args, parameters, parameterSchema) {
    const values = exactObject(parameters);
    if (parameterSchema.kind !== "record")
        throw new Error("invalid reactive parameter schema");
    const schemas = new Map(parameterSchema.fields.map((field) => [field.name, field.schema]));
    const entries = Object.entries(values).sort(([left], [right]) => left.localeCompare(right));
    if (entries.length > 256 || entries.length !== schemas.size) {
        throw new Error("invalid reactive parameters");
    }
    for (const [name, value] of entries) {
        const schema = schemas.get(name);
        if (!SYMBOL.test(name) || schema === undefined) {
            throw new Error("invalid reactive parameter name");
        }
        args.push("--parameter", `${name}=${JSON.stringify(encodeCliReactiveValue(value, schema))}`);
    }
}
function encodeCliReactiveValue(value, schema) {
    switch (schema.kind) {
        case "bool":
            if (typeof value !== "boolean")
                return fail();
            return { type: "bool", value };
        case "i64":
            if (typeof value !== "bigint" || value < -(1n << 63n) || value > (1n << 63n) - 1n)
                return fail();
            return { type: "i64", value: value.toString() };
        case "u64":
            if (typeof value !== "bigint" || value < 0n || value > (1n << 64n) - 1n)
                return fail();
            return { type: "u64", value: value.toString() };
        case "string":
        case "cursor":
            return { type: "string", value: expectBoundedString(value, 262_144) };
        case "uuid":
            if (typeof value !== "string" || !UUID.test(value))
                return fail();
            return { type: "uuid", value };
        case "enum":
            {
                const name = expectSymbol(value);
                const variantId = schema.variants?.[name];
                if (schema.typeId === undefined || variantId === undefined)
                    return fail();
                return { type: "enum", type_id: positiveNumber(schema.typeId), variant_id: positiveNumber(variantId), name };
            }
        case "bytes":
            if (!(value instanceof Uint8Array) || value.byteLength > 1_048_576)
                return fail();
            return { type: "bytes", value: Buffer.from(value).toString("base64") };
        case "date":
            return { type: "date", days_since_unix_epoch: boundedInteger(value, -2_147_483_648, 2_147_483_647) };
        case "timestamp": {
            const timestamp = exactObject(value);
            if (typeof timestamp.seconds !== "bigint"
                || timestamp.seconds < -(1n << 63n)
                || timestamp.seconds > (1n << 63n) - 1n)
                return fail();
            return {
                type: "timestamp",
                seconds: timestamp.seconds.toString(),
                nanos: boundedInteger(timestamp.nanos, 0, 999_999_999),
            };
        }
        case "decimal": {
            const decimal = encodeDecimal(value);
            if (schema.precision === undefined || schema.scale === undefined
                || decimal.scale !== schema.scale
                || (decimal.precision !== undefined && decimal.precision !== schema.precision))
                return fail();
            return { type: "decimal", ...decimal, precision: schema.precision };
        }
        case "money": {
            const money = exactObject(value);
            return {
                type: "money",
                currency: schema.currency === undefined ? fail() : expectCurrency(schema.currency),
                amount: (() => {
                    const amount = encodeDecimal(money.amount);
                    if (money.currency !== schema.currency || schema.precision === undefined || schema.scale === undefined
                        || amount.scale !== schema.scale
                        || (amount.precision !== undefined && amount.precision !== schema.precision))
                        return fail();
                    return { ...amount, precision: schema.precision };
                })(),
            };
        }
        case "limit":
            return { type: "u64", value: String(boundedInteger(value, 1, 500)) };
        case "optional":
        case "list":
        case "record":
            throw new Error("invalid reactive parameter schema");
    }
}
function decodeReactiveConsumerStatus(value) {
    const status = exactObject(value);
    return {
        revision: positiveBigInt(status.revision),
        checkpoint: expectBoundedString(status.checkpoint, 64),
        historyIncarnation: positiveBigInt(status.history_incarnation),
        liveLeases: nonnegativeNumber(status.live_leases),
        retries: nonnegativeNumber(status.retries),
        deadLetters: nonnegativeNumber(status.dead_letters),
    };
}
function decodeEventMutationResult(value) {
    const result = expectSymbol(exactObject(value).result);
    if (result === "applied" || result === "state_changed" || result === "not_found"
        || result === "outstanding_lease" || result === "stale_lease" || result === "lease_expired") {
        return result;
    }
    throw new Error("invalid RiffDB consumer mutation result");
}
function decodeLiveValue(value) {
    const result = exactObject(value);
    const output = { outcome: expectSymbol(result.outcome) };
    for (const rawField of expectArray(result.fields)) {
        const field = exactObject(rawField);
        const name = expectSymbol(field.name);
        const records = expectArray(field.records).map((rawRecord) => {
            const record = exactObject(rawRecord);
            return Object.fromEntries(expectArray(record.fields).map((rawValue) => {
                const item = exactObject(rawValue);
                return [expectSymbol(item.name), decodeTagged(item.value)];
            }));
        });
        const cardinality = boundedInteger(field.cardinality, 1, 3);
        if ((cardinality === 1 && records.length !== 1) || (cardinality === 2 && records.length > 1)) {
            throw new Error("invalid RiffDB live query cardinality");
        }
        output[name] = cardinality === 3 ? records : (records[0] ?? null);
    }
    return output;
}
function decodeLivePatchOperation(value) {
    const operation = exactObject(value);
    const type = expectSymbol(operation.type);
    if (type === "insert" || type === "replace") {
        return {
            type,
            index: boundedInteger(operation.index, 0, 65_535),
            record: decodeLiveRecord(operation.record),
        };
    }
    if (type === "remove") {
        return {
            type,
            index: boundedInteger(operation.index, 0, 65_535),
            key: decodeLiveValueRecord(operation.key),
        };
    }
    if (type === "move") {
        return {
            type,
            from: boundedInteger(operation.from, 0, 65_535),
            to: boundedInteger(operation.to, 0, 65_535),
            key: decodeLiveValueRecord(operation.key),
        };
    }
    throw new Error("invalid RiffDB live patch operation");
}
function decodeLiveRecord(value) {
    const record = exactObject(value);
    expectSymbol(record.entity);
    return decodeLiveValueRecord(record.fields);
}
function decodeLiveValueRecord(value) {
    const record = exactObject(value);
    return Object.fromEntries(Object.entries(record).map(([name, field]) => {
        expectSymbol(name);
        return [name, decodeTagged(field)];
    }));
}
function liveTerminalFrontier(value) {
    if (value === null || value === undefined)
        return {};
    const frontier = exactObject(value);
    return {
        lastApplicationHead: nonnegativeBigInt(frontier.application_head),
        historyIncarnation: positiveBigInt(frontier.history_incarnation),
    };
}
function expectSymbol(value) {
    if (typeof value !== "string" || !SYMBOL.test(value))
        return fail();
    return value;
}
function expectHash(value) {
    if (typeof value !== "string" || !HASH.test(value))
        return fail();
    return value;
}
function expectDecimal(value) {
    if (typeof value !== "string" || !/^-?[0-9]+$/.test(value))
        return fail();
    return value;
}
function expectBoundedString(value, maximum) {
    if (typeof value !== "string" || value.length > maximum)
        return fail();
    return value;
}
function expectCurrency(value) {
    if (typeof value !== "string" || !/^[A-Z]{3}$/.test(value))
        return fail();
    return value;
}
function signedTwosComplement(value) {
    for (let width = 1; width <= 16; width += 1) {
        const bits = BigInt(width * 8);
        const minimum = -(1n << (bits - 1n));
        const maximum = (1n << (bits - 1n)) - 1n;
        if (value < minimum || value > maximum)
            continue;
        let encoded = value < 0n ? (1n << bits) + value : value;
        const bytes = new Uint8Array(width);
        for (let index = width - 1; index >= 0; index -= 1) {
            bytes[index] = Number(encoded & 0xffn);
            encoded >>= 8n;
        }
        return bytes;
    }
    return fail();
}
function positiveNumber(value) {
    const number = typeof value === "string" ? Number(expectDecimal(value)) : value;
    if (!Number.isSafeInteger(number) || number < 1)
        return fail();
    return number;
}
function nonnegativeNumber(value) {
    if (!Number.isSafeInteger(value) || value < 0)
        return fail();
    return value;
}
function positiveBigInt(value) {
    const number = BigInt(expectDecimal(value));
    if (number < 1n)
        return fail();
    return number;
}
function fail() {
    throw new Error("invalid RiffDB application response");
}
export { DRIVER_ERROR_REGISTRY_HASH, DRIVER_PROTOCOL_VERSION, DRIVER_VALUE_REGISTRY_HASH, DriverApplicationError, DriverApplicationTransport, } from "./driver.js";
