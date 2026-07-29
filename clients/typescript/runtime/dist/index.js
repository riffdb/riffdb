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
                moduleHash: request.moduleHash,
                queryName: expectSymbol(rawIdentity.query_name),
            },
            value,
            applicationHead: positiveBigInt(result.application_head),
        };
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
        const result = exactObject(envelope.result);
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
        if (result.commit_sequence !== undefined) {
            return {
                ...typed,
                commitSequence: positiveBigInt(result.commit_sequence),
                ...(result.outcome_uri === undefined
                    ? {}
                    : { outcomeUri: expectBoundedString(result.outcome_uri, 4096) }),
            };
        }
        return typed;
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
    async invoke(args, input, decodeError) {
        const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-"));
        const inputPath = join(directory, "input.json");
        try {
            await writeFile(inputPath, `${JSON.stringify(input)}\n`, { encoding: "utf8", mode: 0o600 });
            const commandArgs = [...args];
            const operation = commandArgs.indexOf("command");
            if (operation >= 0)
                commandArgs.push("--input", inputPath);
            else
                commandArgs.push("--parameters", inputPath);
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
        if (!Array.isArray(value) || (schema.maximum !== undefined && value.length > schema.maximum)) {
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
        case "limit":
            if (!Number.isInteger(value) || value < 1 || value > 500) {
                throw new Error("invalid limit input");
            }
            return value;
        default:
            throw new Error(`generated application scalar is not supported by the CLI transport: ${schema.kind}`);
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
        case "list":
            if (!Array.isArray(input.values))
                return fail();
            return input.values.map(decodeTagged);
        default: return fail();
    }
}
function decodePlain(value, schema) {
    if (schema.kind === "optional")
        return value === null ? null : decodePlain(value, schema.value);
    if (schema.kind === "list") {
        if (!Array.isArray(value) || (schema.maximum !== undefined && value.length > schema.maximum))
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
        default: break;
    }
    return fail();
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
