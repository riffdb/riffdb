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

export type ApplicationValueSchema =
  | { readonly kind: "bool" | "i64" | "u64" | "string" | "uuid" | "bytes" | "date" | "timestamp" | "cursor" | "limit" }
  | { readonly kind: "decimal"; readonly precision?: number; readonly scale?: number }
  | { readonly kind: "money"; readonly precision?: number; readonly scale?: number; readonly currency?: string }
  | { readonly kind: "enum"; readonly typeId?: number; readonly variants?: Readonly<Record<string, number>> }
  | { readonly kind: "optional"; readonly value: ApplicationValueSchema }
  | { readonly kind: "list"; readonly value: ApplicationValueSchema; readonly maximum?: number }
  | { readonly kind: "record"; readonly fields: ReadonlyArray<{ readonly name: string; readonly schema: ApplicationValueSchema; readonly wireId?: number }> };

export interface QueryOptions {
  readonly cursor?: string;
  readonly readAfterCommit?: bigint;
}

interface NamedQueryRequest<P, R> {
  readonly contractLineage: string;
  readonly contractVersion: number;
  readonly contractBundleHash: string;
  readonly moduleHash: string;
  readonly queryName: string;
  readonly planHash: string;
  readonly parameters: P;
  readonly parameterSchema: ApplicationValueSchema;
  readonly resultSchemas: Readonly<Record<string, ApplicationValueSchema>>;
  readonly decodeError: (value: unknown) => Error;
  readonly resultType?: R;
}

interface CommandRequest<I, R> {
  readonly contractLineage: string;
  readonly contractVersion: number;
  readonly commandName: string;
  readonly planHash: string;
  readonly input: I;
  readonly idempotencyKey: string;
  readonly inputSchema: ApplicationValueSchema;
  readonly outcomeSchemas: Readonly<Record<string, ApplicationValueSchema>>;
  readonly decodeError: (value: unknown) => Error;
  readonly outcomeType?: R;
}

export interface QueryResponseIdentity {
  readonly contractLineage: string;
  readonly contractVersion: number;
  readonly contractBundleHash: string;
  readonly moduleHash: string;
  readonly queryName: string;
  readonly planHash: string;
}

export interface TypedQueryResult<T> {
  readonly identity: QueryResponseIdentity;
  readonly value: T;
  readonly applicationHead: bigint;
  readonly nextCursor?: string;
}

export interface TypedCommandResult<T> {
  readonly outcome: T;
  readonly commitSequence?: bigint;
  readonly contractVersion: number;
  readonly planHash: string;
  readonly replayed: boolean;
  readonly outcomeUri?: string;
}

export interface CliApplicationTransportOptions {
  readonly riffdbPath: string;
  readonly endpoint: string;
  readonly credentialFile: string;
}

export interface ReactiveConsumerRequest<P> {
  readonly reactiveModuleHash: string;
  readonly operationName: string;
  readonly parameters: P;
  readonly parameterSchema: ApplicationValueSchema;
  readonly consumerName: string;
}

export interface ReactiveConsumerOptions {
  readonly batchLimit?: number;
  readonly inFlightLimit?: number;
  readonly leaseSeconds?: number;
  readonly maximumWaitMs?: number;
}

export interface ReactiveEventDelivery<E> {
  readonly eventId: string;
  readonly event: E;
  readonly attempt: number;
  readonly leaseToken: string;
  readonly expiresAt: string;
  readonly historyIncarnation: bigint;
}

export interface ReactiveConsumerBatch<E> {
  readonly events: ReadonlyArray<ReactiveEventDelivery<E>>;
  readonly waitTimedOut: boolean;
  readonly status: ReactiveConsumerStatus;
}

export interface ReactiveConsumerStatus {
  readonly revision: bigint;
  readonly checkpoint: string;
  readonly historyIncarnation: bigint;
  readonly liveLeases: number;
  readonly retries: number;
  readonly deadLetters: number;
}

export type ReactiveEventMutationResult =
  | "applied"
  | "state_changed"
  | "not_found"
  | "outstanding_lease"
  | "stale_lease"
  | "lease_expired";

export type LiveQueryPatchOperation =
  | { readonly type: "insert"; readonly index: number; readonly record: Readonly<Record<string, unknown>> }
  | { readonly type: "remove"; readonly index: number; readonly key: Readonly<Record<string, unknown>> }
  | { readonly type: "replace"; readonly index: number; readonly record: Readonly<Record<string, unknown>> }
  | { readonly type: "move"; readonly from: number; readonly to: number; readonly key: Readonly<Record<string, unknown>> };

export type LiveQueryUpdate<T> =
  | { readonly type: "snapshot"; readonly value: T; readonly cursor: string; readonly applicationHead: bigint; readonly historyIncarnation: bigint }
  | { readonly type: "patch"; readonly resultField: string; readonly operations: ReadonlyArray<LiveQueryPatchOperation>; readonly cursor: string; readonly applicationHead: bigint; readonly historyIncarnation: bigint }
  | { readonly type: "reset"; readonly reason: string; readonly value: T; readonly cursor: string; readonly applicationHead: bigint; readonly historyIncarnation: bigint }
  | { readonly type: "checkpoint"; readonly cursor: string; readonly applicationHead: bigint; readonly historyIncarnation: bigint }
  | { readonly type: "terminal"; readonly reason: string; readonly lastApplicationHead?: bigint; readonly historyIncarnation?: bigint };

export class CliApplicationTransport {
  public constructor(private readonly options: CliApplicationTransportOptions) {
    if (options.riffdbPath.length === 0 || options.riffdbPath.length > 4096
      || options.endpoint.length === 0 || options.endpoint.length > 2048
      || options.credentialFile.length === 0 || options.credentialFile.length > 4096) {
      throw new Error("invalid RiffDB application transport configuration");
    }
  }

  public async executeNamedQuery<P, R>(
    request: NamedQueryRequest<P, R>,
    options: QueryOptions = {},
  ): Promise<TypedQueryResult<R>> {
    validateIdentity(request);
    const parameters = encodeValue(request.parameters, request.parameterSchema);
    const args = this.baseArguments();
    args.push(
      "query", "run-named", request.queryName,
      "--module-hash", request.moduleHash,
      "--contract-lineage", request.contractLineage,
      "--contract-version", String(request.contractVersion),
    );
    if (options.cursor !== undefined) args.push("--cursor", options.cursor);
    if (options.readAfterCommit !== undefined) {
      if (options.readAfterCommit < 1n) throw new Error("invalid read-after-commit fence");
      args.push("--read-after-commit", options.readAfterCommit.toString());
    }
    const envelope = await this.invoke(args, parameters, request.decodeError);
    const result = exactObject(envelope.result);
    const rawIdentity = exactObject(result.identity);
    const outcome = expectSymbol(result.outcome);
    const resultSchema = request.resultSchemas[outcome];
    if (resultSchema === undefined) throw new Error("RiffDB application response has an unknown outcome");
    const value = decodePlain(
      { outcome, ...normalizeQueryFields(result.fields) },
      {
        kind: "record",
        fields: [
          { name: "outcome", schema: { kind: "string" } },
          ...recordFields(resultSchema),
        ],
      },
    ) as R;
    const typed: TypedQueryResult<R> = {
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

  public async executeCommand<I, R>(
    request: CommandRequest<I, R>,
    attemptBudget: number,
  ): Promise<TypedCommandResult<R>> {
    validateIdentity(request);
    if (!Number.isInteger(attemptBudget) || attemptBudget < 1 || attemptBudget > 10) {
      throw new Error("invalid command attempt budget");
    }
    const input = encodeValue(request.input, request.inputSchema);
    const args = this.baseArguments(attemptBudget);
    args.push(
      "command", "run", request.commandName,
      "--expected-version", String(request.contractVersion),
    );
    const envelope = await this.invoke(args, input, request.decodeError);
    const result = exactObject(envelope.result);
    if (expectHash(result.plan_hash) !== request.planHash) {
      throw new Error("RiffDB application identity mismatch");
    }
    const outcomeName = expectSymbol(result.outcome_type);
    const schema = request.outcomeSchemas[outcomeName];
    if (schema === undefined) throw new Error("RiffDB application response has an unknown outcome");
    const payload = decodeWire(result.outcome, schema);
    const outcome = { outcome: outcomeName, ...exactObject(payload) } as R;
    const typed: TypedCommandResult<R> = {
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

  public async *consumeEventStream<P, E>(
    request: ReactiveConsumerRequest<P>,
    options: ReactiveConsumerOptions = {},
  ): AsyncIterable<ReactiveConsumerBatch<E>> {
    validateReactiveRequest(request);
    const batchLimit = boundedInteger(options.batchLimit ?? 1, 1, 64);
    const inFlightLimit = boundedInteger(options.inFlightLimit ?? 16, 1, 64);
    const leaseSeconds = boundedInteger(options.leaseSeconds ?? 60, 5, 900);
    const maximumWaitMs = boundedInteger(options.maximumWaitMs ?? 30_000, 0, 30_000);
    while (true) {
      const args = this.reactiveArguments(request);
      args.push(
        "event", "consume",
        "--module-hash", request.reactiveModuleHash,
        "--operation", request.operationName,
        "--consumer-name", request.consumerName,
        "--batch-limit", String(batchLimit),
        "--in-flight-limit", String(inFlightLimit),
        "--lease-seconds", String(leaseSeconds),
        "--wait-nanos", String(maximumWaitMs * 1_000_000),
      );
      addReactiveParameters(args, request.parameters, request.parameterSchema);
      const envelope = await this.invokeArguments(args);
      const result = exactObject(envelope.result);
      const status = decodeReactiveConsumerStatus(result.status);
      const historyIncarnation = status.historyIncarnation;
      const events = expectArray(result.events).map((value): ReactiveEventDelivery<E> => {
        const delivery = exactObject(value);
        const fields = Object.fromEntries(expectArray(delivery.fields).map((field) => {
          const item = exactObject(field);
          return [expectSymbol(item.name), decodeTagged(item.value)];
        }));
        const expiration = exactObject(delivery.expires_at);
        return {
          eventId: expectBoundedString(delivery.event_id, 64),
          event: { type: expectSymbol(delivery.event_name), ...fields } as E,
          attempt: positiveNumber(delivery.attempt),
          leaseToken: expectHash(delivery.lease_token),
          expiresAt: `${expectBoundedString(expiration.seconds, 32)}.${String(boundedInteger(expiration.nanos, 0, 999_999_999)).padStart(9, "0")}`,
          historyIncarnation,
        };
      });
      yield { events, waitTimedOut: result.wait_timed_out === true, status };
    }
  }

  public async acknowledgeEvent<P>(
    request: ReactiveConsumerRequest<P>,
    delivery: ReactiveEventDelivery<unknown>,
  ): Promise<ReactiveEventMutationResult> {
    return this.mutateEventLease("ack", request, delivery);
  }

  public async negativeAcknowledgeEvent<P>(
    request: ReactiveConsumerRequest<P>,
    delivery: ReactiveEventDelivery<unknown>,
    retryDelayMs = 0,
  ): Promise<ReactiveEventMutationResult> {
    return this.mutateEventLease("nack", request, delivery, retryDelayMs);
  }

  public async seekEventConsumer<P>(
    request: ReactiveConsumerRequest<P>,
    checkpoint: string,
  ): Promise<ReactiveEventMutationResult> {
    validateReactiveRequest(request);
    const args = this.reactiveArguments(request);
    args.push(
      "event", "seek", "--module-hash", request.reactiveModuleHash,
      "--operation", request.operationName, "--consumer-name", request.consumerName,
      "--checkpoint", expectBoundedString(checkpoint, 64),
    );
    addReactiveParameters(args, request.parameters, request.parameterSchema);
    return decodeEventMutationResult((await this.invokeArguments(args)).result);
  }

  public async eventConsumerStatus<P>(
    request: ReactiveConsumerRequest<P>,
  ): Promise<ReactiveConsumerStatus | undefined> {
    validateReactiveRequest(request);
    const args = this.reactiveArguments(request);
    args.push(
      "event", "status", "--module-hash", request.reactiveModuleHash,
      "--operation", request.operationName, "--consumer-name", request.consumerName,
    );
    addReactiveParameters(args, request.parameters, request.parameterSchema);
    const result = exactObject((await this.invokeArguments(args)).result);
    if (result.found !== true) return undefined;
    return decodeReactiveConsumerStatus(result.status);
  }

  public async *watchNamedQuery<P, T>(request: {
    readonly reactiveModuleHash: string;
    readonly operationName: string;
    readonly parameters: P;
    readonly parameterSchema: ApplicationValueSchema;
    readonly cursor?: string;
  }): AsyncIterable<LiveQueryUpdate<T>> {
    if (!HASH.test(request.reactiveModuleHash) || !SYMBOL.test(request.operationName)) {
      throw new Error("invalid reactive operation identity");
    }
    let cursor = request.cursor;
    while (true) {
      const args = this.baseArguments();
      args.push(
        "query", "watch", request.operationName,
        "--module-hash", request.reactiveModuleHash,
      );
      addReactiveParameters(args, request.parameters, request.parameterSchema);
      if (cursor !== undefined) args.push("--cursor", cursor);
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
        yield { type, value: decodeLiveValue(result.result) as T, cursor, applicationHead, historyIncarnation };
      } else if (type === "reset") {
        yield { type, reason: expectSymbol(result.reason), value: decodeLiveValue(result.result) as T, cursor, applicationHead, historyIncarnation };
      } else if (type === "patch") {
        yield { type, resultField: expectSymbol(result.result_field), operations: expectArray(result.operations).map(decodeLivePatchOperation), cursor, applicationHead, historyIncarnation };
      } else if (type === "checkpoint") {
        yield { type, cursor, applicationHead, historyIncarnation };
      } else {
        throw new Error("invalid RiffDB live query update");
      }
    }
  }

  private async mutateEventLease<P>(
    action: "ack" | "nack",
    request: ReactiveConsumerRequest<P>,
    delivery: ReactiveEventDelivery<unknown>,
    retryDelayMs = 0,
  ): Promise<ReactiveEventMutationResult> {
    validateReactiveRequest(request);
    const delay = boundedInteger(retryDelayMs, 0, 3_600_000);
    const args = this.reactiveArguments(request);
    args.push(
      "event", action, "--module-hash", request.reactiveModuleHash,
      "--operation", request.operationName, "--consumer-name", request.consumerName,
      "--event-id", expectBoundedString(delivery.eventId, 64),
      "--lease-token", expectHash(delivery.leaseToken),
      "--history-incarnation", delivery.historyIncarnation.toString(),
    );
    if (action === "nack") args.push("--retry-delay-nanos", String(delay * 1_000_000));
    addReactiveParameters(args, request.parameters, request.parameterSchema);
    return decodeEventMutationResult((await this.invokeArguments(args)).result);
  }

  private reactiveArguments<P>(request: ReactiveConsumerRequest<P>): string[] {
    validateReactiveRequest(request);
    return this.baseArguments();
  }

  private baseArguments(attemptBudget?: number): string[] {
    const args = [
      "--endpoint", this.options.endpoint,
      "--output", "json",
      "--credential-file", this.options.credentialFile,
    ];
    if (attemptBudget !== undefined) args.push("--max-attempts", String(attemptBudget));
    return args;
  }

  private async invoke(
    args: string[],
    input: unknown,
    decodeError: (value: unknown) => Error,
  ): Promise<Record<string, unknown>> {
    const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-"));
    const inputPath = join(directory, "input.json");
    try {
      await writeFile(inputPath, `${JSON.stringify(input)}\n`, { encoding: "utf8", mode: 0o600 });
      const commandArgs = [...args];
      const operation = commandArgs.indexOf("command");
      if (operation >= 0) commandArgs.push("--input", inputPath);
      else commandArgs.push("--parameters", inputPath);
      let stdout: string;
      try {
        ({ stdout } = await executeFile(this.options.riffdbPath, commandArgs, {
          encoding: "utf8",
          maxBuffer: MAX_OUTPUT_BYTES,
          timeout: 35_000,
        }));
      } catch (error) {
        const candidate = exactObject(error);
        stdout = typeof candidate.stdout === "string" ? candidate.stdout : "";
      }
      const envelope = exactObject(JSON.parse(stdout) as unknown);
      if (envelope.schema !== "riffdb.cli.output/v1") throw new Error("invalid RiffDB CLI envelope");
      if (envelope.ok !== true) {
        const raw = exactObject(envelope.error);
        if (raw.type === "application") throw decodeError(normalizeError(raw));
        throw new Error("RiffDB application transport failed");
      }
      return envelope;
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  private async invokeArguments(args: string[]): Promise<Record<string, unknown>> {
    let stdout: string;
    try {
      ({ stdout } = await executeFile(this.options.riffdbPath, args, {
        encoding: "utf8", maxBuffer: MAX_OUTPUT_BYTES, timeout: 35_000,
      }));
    } catch (error) {
      const candidate = exactObject(error);
      stdout = typeof candidate.stdout === "string" ? candidate.stdout : "";
    }
    const envelope = exactObject(JSON.parse(stdout) as unknown);
    if (envelope.schema !== "riffdb.cli.output/v1") throw new Error("invalid RiffDB CLI envelope");
    if (envelope.ok !== true) throw new Error("RiffDB reactive operation failed");
    return envelope;
  }
}

function validateIdentity(value: {
  readonly contractLineage: string;
  readonly contractVersion: number;
  readonly moduleHash?: string;
  readonly planHash?: string;
  readonly queryName?: string;
  readonly commandName?: string;
}): void {
  expectSymbol(value.contractLineage);
  positiveNumber(value.contractVersion);
  if (value.moduleHash !== undefined) expectHash(value.moduleHash);
  if (value.planHash !== undefined) expectHash(value.planHash);
  if (value.queryName !== undefined) expectSymbol(value.queryName);
  if (value.commandName !== undefined) expectSymbol(value.commandName);
}

function encodeValue(value: unknown, schema: ApplicationValueSchema): unknown {
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
    const output: Record<string, unknown> = {};
    for (const field of schema.fields) {
      const fieldValue = input[field.name];
      if (fieldValue === undefined && field.schema.kind === "optional") continue;
      if (fieldValue === undefined) continue;
      output[field.name] = encodeValue(fieldValue, field.schema);
    }
    return output;
  }
  switch (schema.kind) {
    case "uuid":
      if (typeof value !== "string" || !UUID.test(value)) throw new Error("invalid UUID input");
      return { $uuid: value };
    case "enum":
      return { $enum: expectSymbol(value) };
    case "string":
    case "cursor":
      return expectBoundedString(value, 262_144);
    case "bool":
      if (typeof value !== "boolean") throw new Error("invalid boolean input");
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
    case "decimal":
      return { $decimal: encodeDecimal(value) };
    case "money": {
      const input = exactObject(value);
      return {
        $money: {
          currency: expectCurrency(input.currency),
          amount: encodeDecimal(input.amount),
        },
      };
    }
    case "bytes":
      if (!(value instanceof Uint8Array) || value.byteLength > 1_048_576) {
        throw new Error("invalid bytes input");
      }
      return { $bytes: Buffer.from(value).toString("base64") };
    case "date":
      if (!Number.isInteger(value) || (value as number) < -2_147_483_648 || (value as number) > 2_147_483_647) {
        throw new Error("invalid date input");
      }
      return { $date: value };
    case "timestamp": {
      const input = exactObject(value);
      const seconds = input.seconds;
      const nanos = input.nanos;
      if (typeof seconds !== "bigint" || seconds < -(1n << 63n) || seconds > (1n << 63n) - 1n
          || !Number.isInteger(nanos) || (nanos as number) < 0 || (nanos as number) > 999_999_999) {
        throw new Error("invalid timestamp input");
      }
      return { $timestamp: { seconds: seconds.toString(), nanos } };
    }
    case "limit":
      if (!Number.isInteger(value) || (value as number) < 1 || (value as number) > 500) {
        throw new Error("invalid limit input");
      }
      return value;
    default:
      throw new Error("generated application scalar is not supported by the CLI transport");
  }
}

function normalizeQueryFields(value: unknown): Record<string, unknown> {
  if (!Array.isArray(value) || value.length > 4096) throw new Error("invalid RiffDB query fields");
  const output: Record<string, unknown> = {};
  for (const item of value) {
    const field = exactObject(item);
    const name = expectSymbol(field.name);
    if (name in output || !Array.isArray(field.records)) throw new Error("invalid RiffDB query fields");
    const records = field.records.map((record) => {
      const raw = exactObject(record);
      if (!Array.isArray(raw.fields)) throw new Error("invalid RiffDB query record");
      const result: Record<string, unknown> = {};
      for (const item of raw.fields) {
        const entry = exactObject(item);
        const fieldName = expectSymbol(entry.name);
        if (fieldName in result) throw new Error("duplicate RiffDB query field");
        result[fieldName] = decodeTagged(entry.value);
      }
      return result;
    });
    switch (field.cardinality) {
      case "one":
        if (records.length !== 1) throw new Error("invalid one cardinality");
        output[name] = records[0];
        break;
      case "maybe":
        if (records.length > 1) throw new Error("invalid maybe cardinality");
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

function decodeTagged(value: unknown): unknown {
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
      if (!Array.isArray(input.values)) return fail();
      return input.values.map(decodeTagged);
    default: return fail();
  }
}

function encodeDecimal(value: unknown): {
  readonly coefficient_twos_complement: string;
  readonly scale: number;
  readonly precision?: number;
} {
  const input = exactObject(value);
  const coefficient = input.coefficientTwosComplement;
  if (!(coefficient instanceof Uint8Array) || coefficient.byteLength < 1 || coefficient.byteLength > 16
      || !Number.isInteger(input.scale) || (input.scale as number) < 0
      || (input.scale as number) > 4_294_967_295) {
    throw new Error("invalid decimal input");
  }
  const output: {
    coefficient_twos_complement: string;
    scale: number;
    precision?: number;
  } = {
    coefficient_twos_complement: Buffer.from(coefficient).toString("base64"),
    scale: input.scale as number,
  };
  if (input.precision !== undefined) {
    if (!Number.isInteger(input.precision) || (input.precision as number) < 1
        || (input.precision as number) > 4_294_967_295) {
      throw new Error("invalid decimal precision");
    }
    output.precision = input.precision as number;
  }
  return output;
}

function decodeDecimal(input: Record<string, unknown>): {
  readonly coefficientTwosComplement: Uint8Array;
  readonly scale: number;
  readonly precision?: number;
} {
  const coefficient = Uint8Array.from(
    Buffer.from(expectBoundedString(input.coefficient_twos_complement, 24), "base64"),
  );
  const scale = nonnegativeNumber(input.scale);
  if (coefficient.byteLength < 1 || coefficient.byteLength > 16) return fail();
  if (input.precision === undefined) return { coefficientTwosComplement: coefficient, scale };
  const precision = positiveNumber(input.precision);
  return { coefficientTwosComplement: coefficient, scale, precision };
}

function decodePlain(value: unknown, schema: ApplicationValueSchema): unknown {
  if (schema.kind === "optional") return value === null ? null : decodePlain(value, schema.value);
  if (schema.kind === "list") {
    if (!Array.isArray(value) || (schema.maximum !== undefined && value.length > schema.maximum)) return fail();
    return value.map((item) => decodePlain(item, schema.value));
  }
  if (schema.kind === "record") {
    const input = exactObject(value);
    const output: Record<string, unknown> = {};
    if (Object.keys(input).length !== schema.fields.length) return fail();
    for (const field of schema.fields) output[field.name] = decodePlain(input[field.name], field.schema);
    return output;
  }
  switch (schema.kind) {
    case "bool": if (typeof value === "boolean") return value; break;
    case "i64":
    case "u64": if (typeof value === "bigint") return value; break;
    case "string":
    case "enum":
    case "cursor": if (typeof value === "string") return value; break;
    case "uuid": if (typeof value === "string" && UUID.test(value)) return value; break;
    case "date":
    case "limit": if (Number.isInteger(value)) return value; break;
    case "timestamp": {
      const input = exactObject(value);
      if (typeof input.seconds === "bigint" && Number.isInteger(input.nanos)) return value;
      break;
    }
    case "bytes": if (value instanceof Uint8Array) return value; break;
    case "decimal": {
      const decimal = encodeDecimal(value);
      if ((schema.precision !== undefined && decimal.precision !== schema.precision)
        || (schema.scale !== undefined && decimal.scale !== schema.scale)) return fail();
      return value;
    }
    case "money": {
      const input = exactObject(value);
      const currency = expectCurrency(input.currency);
      const amount = encodeDecimal(input.amount);
      if ((schema.currency !== undefined && currency !== schema.currency)
        || (schema.precision !== undefined && amount.precision !== schema.precision)
        || (schema.scale !== undefined && amount.scale !== schema.scale)) return fail();
      return value;
    }
    default: break;
  }
  return fail();
}

function decodeWire(value: unknown, schema: ApplicationValueSchema): unknown {
  if (schema.kind !== "record") return decodePlain(decodeTagged(value), schema);
  const input = exactObject(value);
  if (input.type !== "record" || !Array.isArray(input.fields)) return fail();
  const fields = new Map<number, unknown>();
  for (const item of input.fields) {
    const field = exactObject(item);
    const id = positiveNumber(field.field_id);
    if (fields.has(id)) return fail();
    fields.set(id, field.value);
  }
  const output: Record<string, unknown> = {};
  for (const field of schema.fields) {
    if (field.wireId === undefined) return fail();
    const raw = fields.get(field.wireId);
    if (raw === undefined) return fail();
    output[field.name] = decodeWire(raw, field.schema);
    fields.delete(field.wireId);
  }
  if (fields.size !== 0) return fail();
  return output;
}

function normalizeError(input: Record<string, unknown>): unknown {
  const output: Record<string, unknown> = {
    type: input.type,
    code: input.code,
    message: input.message,
    category: input.category,
    recoveryAction: input.recovery_action,
    operation: input.operation,
    symbolPath: input.symbol_path ?? [],
    fixes: input.fixes,
  };
  if (input.contract_lineage !== undefined) output.contractLineage = input.contract_lineage;
  if (input.contract_version !== undefined) output.contractVersion = Number(expectDecimal(input.contract_version));
  if (input.operation_symbol !== undefined) output.operationSymbol = input.operation_symbol;
  if (input.source_span !== undefined) {
    const span = exactObject(input.source_span);
    output.sourceSpan = { start: Number(expectDecimal(span.start)), end: Number(expectDecimal(span.end)) };
  }
  if (input.trace_id !== undefined) output.traceId = input.trace_id;
  if (input.incident_id !== undefined) output.incidentId = input.incident_id;
  return output;
}

function recordFields(schema: ApplicationValueSchema): ReadonlyArray<{ readonly name: string; readonly schema: ApplicationValueSchema }> {
  if (schema.kind !== "record") return fail();
  return schema.fields;
}

function exactObject(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return fail();
  return value as Record<string, unknown>;
}

function expectArray(value: unknown): ReadonlyArray<unknown> {
  if (!Array.isArray(value) || value.length > 100_000) throw new Error("invalid RiffDB collection");
  return value;
}

function boundedInteger(value: unknown, minimum: number, maximum: number): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < minimum || value > maximum) {
    throw new Error("invalid bounded integer");
  }
  return value;
}

function nonnegativeBigInt(value: unknown): bigint {
  if (typeof value !== "string" || !/^(0|[1-9][0-9]{0,19})$/.test(value)) {
    throw new Error("invalid nonnegative integer");
  }
  return BigInt(value);
}

function validateReactiveRequest<P>(request: ReactiveConsumerRequest<P>): void {
  if (!HASH.test(request.reactiveModuleHash)
    || !SYMBOL.test(request.operationName)
    || !/^[A-Za-z][A-Za-z0-9_-]{0,63}$/.test(request.consumerName)) {
    throw new Error("invalid reactive consumer identity");
  }
}

function addReactiveParameters<P>(
  args: string[],
  parameters: P,
  parameterSchema: ApplicationValueSchema,
): void {
  const values = exactObject(parameters);
  if (parameterSchema.kind !== "record") throw new Error("invalid reactive parameter schema");
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

function encodeCliReactiveValue(value: unknown, schema: ApplicationValueSchema): unknown {
  switch (schema.kind) {
    case "bool":
      if (typeof value !== "boolean") return fail();
      return { type: "bool", value };
    case "i64":
      if (typeof value !== "bigint" || value < -(1n << 63n) || value > (1n << 63n) - 1n) return fail();
      return { type: "i64", value: value.toString() };
    case "u64":
      if (typeof value !== "bigint" || value < 0n || value > (1n << 64n) - 1n) return fail();
      return { type: "u64", value: value.toString() };
    case "string":
    case "cursor":
      return { type: "string", value: expectBoundedString(value, 262_144) };
    case "uuid":
      if (typeof value !== "string" || !UUID.test(value)) return fail();
      return { type: "uuid", value };
    case "enum":
      {
        const name = expectSymbol(value);
        const variantId = schema.variants?.[name];
        if (schema.typeId === undefined || variantId === undefined) return fail();
        return { type: "enum", type_id: positiveNumber(schema.typeId), variant_id: positiveNumber(variantId), name };
      }
    case "bytes":
      if (!(value instanceof Uint8Array) || value.byteLength > 1_048_576) return fail();
      return { type: "bytes", value: Buffer.from(value).toString("base64") };
    case "date":
      return { type: "date", days_since_unix_epoch: boundedInteger(value, -2_147_483_648, 2_147_483_647) };
    case "timestamp": {
      const timestamp = exactObject(value);
      if (typeof timestamp.seconds !== "bigint"
        || timestamp.seconds < -(1n << 63n)
        || timestamp.seconds > (1n << 63n) - 1n) return fail();
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
        || (decimal.precision !== undefined && decimal.precision !== schema.precision)) return fail();
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
            || (amount.precision !== undefined && amount.precision !== schema.precision)) return fail();
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

function decodeReactiveConsumerStatus(value: unknown): ReactiveConsumerStatus {
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

function decodeEventMutationResult(value: unknown): ReactiveEventMutationResult {
  const result = expectSymbol(exactObject(value).result);
  if (result === "applied" || result === "state_changed" || result === "not_found"
    || result === "outstanding_lease" || result === "stale_lease" || result === "lease_expired") {
    return result;
  }
  throw new Error("invalid RiffDB consumer mutation result");
}

function decodeLiveValue(value: unknown): unknown {
  const result = exactObject(value);
  const output: Record<string, unknown> = { outcome: expectSymbol(result.outcome) };
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

function decodeLivePatchOperation(value: unknown): LiveQueryPatchOperation {
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

function decodeLiveRecord(value: unknown): Readonly<Record<string, unknown>> {
  const record = exactObject(value);
  expectSymbol(record.entity);
  return decodeLiveValueRecord(record.fields);
}

function decodeLiveValueRecord(value: unknown): Readonly<Record<string, unknown>> {
  const record = exactObject(value);
  return Object.fromEntries(Object.entries(record).map(([name, field]) => {
    expectSymbol(name);
    return [name, decodeTagged(field)];
  }));
}

function liveTerminalFrontier(value: unknown): {
  readonly lastApplicationHead?: bigint;
  readonly historyIncarnation?: bigint;
} {
  if (value === null || value === undefined) return {};
  const frontier = exactObject(value);
  return {
    lastApplicationHead: nonnegativeBigInt(frontier.application_head),
    historyIncarnation: positiveBigInt(frontier.history_incarnation),
  };
}
function expectSymbol(value: unknown): string {
  if (typeof value !== "string" || !SYMBOL.test(value)) return fail();
  return value;
}
function expectHash(value: unknown): string {
  if (typeof value !== "string" || !HASH.test(value)) return fail();
  return value;
}
function expectDecimal(value: unknown): string {
  if (typeof value !== "string" || !/^-?[0-9]+$/.test(value)) return fail();
  return value;
}
function expectBoundedString(value: unknown, maximum: number): string {
  if (typeof value !== "string" || value.length > maximum) return fail();
  return value;
}
function expectCurrency(value: unknown): string {
  if (typeof value !== "string" || !/^[A-Z]{3}$/.test(value)) return fail();
  return value;
}
function positiveNumber(value: unknown): number {
  const number = typeof value === "string" ? Number(expectDecimal(value)) : value;
  if (!Number.isSafeInteger(number) || (number as number) < 1) return fail();
  return number as number;
}
function nonnegativeNumber(value: unknown): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) return fail();
  return value as number;
}
function positiveBigInt(value: unknown): bigint {
  const number = BigInt(expectDecimal(value));
  if (number < 1n) return fail();
  return number;
}
function fail(): never {
  throw new Error("invalid RiffDB application response");
}
