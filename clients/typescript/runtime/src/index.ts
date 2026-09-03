import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

import {
  DriverApplicationTransport,
  type DriverApplicationError,
  type DriverCompactQueryResult,
  type DriverPackedQueryResult,
  type DriverOperation,
  type DriverValue,
} from "./driver.js";

const executeFile = promisify(execFile);
const MAX_OUTPUT_BYTES = 4_194_304;
const HASH = /^[0-9a-f]{64}$/;
const SYMBOL = /^[A-Za-z][A-Za-z0-9_.-]{0,255}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export type ApplicationValueSchema =
  | { readonly kind: "bool" | "i64" | "u64" | "string" | "uuid" | "bytes" | "date" | "timestamp" | "cursor" | "limit"; readonly maximumBytes?: number }
  | { readonly kind: "vector"; readonly dimension: number }
  | { readonly kind: "decimal"; readonly precision?: number; readonly scale?: number }
  | { readonly kind: "money"; readonly precision?: number; readonly scale?: number; readonly currency?: string }
  | { readonly kind: "enum"; readonly typeId?: number; readonly variants?: Readonly<Record<string, number>> }
  | { readonly kind: "optional"; readonly value: ApplicationValueSchema }
  | { readonly kind: "list"; readonly value: ApplicationValueSchema; readonly minimum?: number; readonly maximum?: number; readonly aggregateCanonicalElementBytes?: number }
  | { readonly kind: "record"; readonly fields: ReadonlyArray<{ readonly name: string; readonly schema: ApplicationValueSchema; readonly wireId?: number }> };

export type InputBudgetCause = "collection_count" | "individual_value_bytes" | "aggregate_canonical_element_bytes";
export interface InputBudgetPath { readonly collection: string; readonly index?: number; readonly leaf?: string }
export class InputBudgetError extends Error {
  public constructor(public override readonly cause: InputBudgetCause, public readonly path: InputBudgetPath) {
    super("generated command input exceeds its compiled budget");
    this.name = "InputBudgetError";
  }
}

/** One exact fixed-point value accepted by generated decimal fields. */
export interface ExactDecimalValue {
  readonly coefficientTwosComplement: Uint8Array;
  readonly scale: number;
  readonly precision: number;
}

/** One exact currency value accepted by generated `money<CURRENCY>` fields. */
export interface ExactMoneyValue<Currency extends string = string> {
  readonly currency: Currency;
  readonly amount: ExactDecimalValue;
}

/**
 * Constructs an exact fixed-point value from decimal text without using a
 * JavaScript floating-point number.
 */
export function exactDecimal(value: string, precision: number, scale: number): ExactDecimalValue {
  if (!Number.isInteger(precision) || precision < 1 || precision > 38
      || !Number.isInteger(scale) || scale < 0 || scale > precision) {
    throw new Error("invalid exact decimal type");
  }
  if (typeof value !== "string" || value.length < 1 || value.length > 128) {
    throw new Error("invalid exact decimal text");
  }
  const match = /^(-?)([0-9]+)(?:\.([0-9]+))?$/.exec(value);
  if (match === null) throw new Error("invalid exact decimal text");
  const fractional = match[3] ?? "";
  if (fractional.length > scale) throw new Error("exact decimal exceeds scale");
  const magnitudeText = `${match[2]}${fractional.padEnd(scale, "0")}`;
  let coefficient = BigInt(magnitudeText);
  if (match[1] === "-" && coefficient !== 0n) coefficient = -coefficient;
  const limit = 10n ** BigInt(precision);
  if (coefficient <= -limit || coefficient >= limit) throw new Error("exact decimal exceeds precision");
  return {
    coefficientTwosComplement: signedTwosComplement(coefficient),
    scale,
    precision,
  };
}

/** Constructs the exact precision-38, scale-2 value used by `money<CURRENCY>`. */
export function exactMoney<const Currency extends string>(
  currency: Currency,
  value: string,
): ExactMoneyValue<Currency> {
  if (!/^[A-Z]{3}$/.test(currency)) throw new Error("invalid exact money currency");
  return { currency, amount: exactDecimal(value, 38, 2) };
}

export type QueryConsistency = "admissionHead";

export interface QueryOptions {
  readonly cursor?: string;
  readonly readAfterCommit?: bigint;
  readonly consistency?: QueryConsistency;
}

interface NamedQueryRequest<P, R> {
  readonly driverOperation?: DriverOperation;
  readonly contractLineage: string;
  readonly contractVersion: number;
  readonly contractBundleHash: string;
  readonly moduleHash: string;
  readonly queryName: string;
  readonly planHash: string;
  readonly parameters: P;
  readonly parameterSchema: ApplicationValueSchema;
  readonly resultSchemas: Readonly<Record<string, ApplicationValueSchema>>;
  readonly compactDecoder?: (value: DriverCompactQueryResult) => R;
  readonly packedDecoder?: (value: DriverPackedQueryResult) => R;
  readonly decodeError: (value: unknown) => Error;
  readonly resultType?: R;
}

interface CommandRequest<I, R> {
  readonly driverOperation?: DriverOperation;
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

interface VectorInspectionRequest<P, R> {
  readonly driverOperation?: DriverOperation;
  readonly contractLineage: string;
  readonly contractVersion: number;
  readonly contractBundleHash: string;
  readonly entity: string;
  readonly field: string;
  readonly inspectionKind: "staleness" | "model_versions";
  readonly partition: P;
  readonly partitionSchema: ApplicationValueSchema;
  readonly limit: number;
  readonly resultType?: R;
}

interface VectorInspectionOptions { readonly cursor?: string; }

interface TypedVectorInspectionResult<T> {
  readonly value: T;
  readonly applicationHead?: bigint;
  readonly nextCursor?: string;
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
  readonly driverOperations?: Readonly<Record<string, DriverOperation>>;
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
  readonly signal?: AbortSignal;
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

export interface ContextualHydration {
  readonly name: string;
  readonly outcome: string;
  readonly fields: Readonly<Record<string, unknown>>;
}

export interface ContextualReaction {
  readonly name: string;
  readonly commandName: string;
  readonly commandId: number;
  readonly causationToken: string;
}

export interface ContextualWorkItem<E> {
  readonly delivery: ReactiveEventDelivery<E>;
  readonly contextHead: bigint;
  readonly hydrations: ReadonlyArray<ContextualHydration>;
  readonly availableReactions: ReadonlyArray<ContextualReaction>;
}

export interface ContextualBatch<E> {
  readonly items: ReadonlyArray<ContextualWorkItem<E>>;
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
    if (options.consistency === "admissionHead") args.push("--consistency", "admission-head");
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
    return decodeTypedCommandResult(exactObject(envelope.result), request);
  }

  public async executeVectorInspection<P, R>(
    request: VectorInspectionRequest<P, R>,
    options: VectorInspectionOptions = {},
  ): Promise<TypedVectorInspectionResult<R>> {
    validateIdentity(request);
    const limit = boundedInteger(request.limit, 1, 500);
    const partition = JSON.stringify(encodeValue(request.partition, request.partitionSchema));
    const args = this.baseArguments();
    args.push(
      "query", "inspect-vector", request.entity, request.field,
      "--partition", partition,
      "--kind", request.inspectionKind === "staleness" ? "stale" : "outdated",
      "--limit", String(limit),
      "--contract-lineage", request.contractLineage,
      "--contract-version", String(request.contractVersion),
    );
    if (options.cursor !== undefined) args.push("--cursor-hex", options.cursor);
    const envelope = await this.invokeArguments(args);
    if (envelope.ok !== true) throw new Error("RiffDB vector inspection failed");
    const value = decodePlainVectorInspection(exactObject(envelope.result)) as R;
    const raw = exactObject(envelope.result);
    return {
      value,
      ...(raw.observed_frontier === null || raw.observed_frontier === undefined
        ? {} : { applicationHead: positiveBigInt(raw.observed_frontier) }),
      ...(raw.next_cursor === null || raw.next_cursor === undefined
        ? {} : { nextCursor: expectBoundedString(raw.next_cursor, 32) }),
    };
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

  public async consumeContextualSubscription<P, E>(
    request: ReactiveConsumerRequest<P>,
    maximumWaitMs = 30_000,
  ): Promise<ContextualBatch<E>> {
    validateReactiveRequest(request);
    const wait = boundedInteger(maximumWaitMs, 0, 30_000);
    const args = this.reactiveArguments(request);
    args.push(
      "contextual", "next", "--module-hash", request.reactiveModuleHash,
      "--operation", request.operationName, "--consumer-name", request.consumerName,
      "--wait-nanos", String(wait * 1_000_000),
    );
    addReactiveParameters(args, request.parameters, request.parameterSchema);
    const result = exactObject((await this.invokeArguments(args)).result);
    return {
      items: expectArray(result.items).map((value) => decodeContextualItem<E>(value)),
      waitTimedOut: result.wait_timed_out === true,
      status: decodeReactiveConsumerStatus(result.status),
    };
  }

  public async acknowledgeContextualItem<P>(
    request: ReactiveConsumerRequest<P>,
    item: ContextualWorkItem<unknown>,
  ): Promise<ReactiveEventMutationResult> {
    return this.mutateContextualItem("ack", request, item);
  }

  public async negativeAcknowledgeContextualItem<P>(
    request: ReactiveConsumerRequest<P>,
    item: ContextualWorkItem<unknown>,
    retryDelayMs = 0,
  ): Promise<ReactiveEventMutationResult> {
    return this.mutateContextualItem("nack", request, item, retryDelayMs);
  }

  public async contextualSubscriptionStatus<P>(
    request: ReactiveConsumerRequest<P>,
  ): Promise<ReactiveConsumerStatus | undefined> {
    validateReactiveRequest(request);
    const args = this.reactiveArguments(request);
    args.push(
      "contextual", "status", "--module-hash", request.reactiveModuleHash,
      "--operation", request.operationName, "--consumer-name", request.consumerName,
    );
    addReactiveParameters(args, request.parameters, request.parameterSchema);
    const result = exactObject((await this.invokeArguments(args)).result);
    if (result.found !== true) return undefined;
    return decodeReactiveConsumerStatus(result.status);
  }

  public async executeContextualReaction<P, I, R>(
    request: ReactiveConsumerRequest<P>,
    reaction: ContextualReaction,
    command: CommandRequest<I, R>,
  ): Promise<TypedCommandResult<R>> {
    validateReactiveRequest(request);
    validateIdentity(command);
    if (reaction.commandName !== command.commandName) {
      throw new Error("contextual reaction command identity mismatch");
    }
    const input = encodeValue(command.input, command.inputSchema);
    const args = this.reactiveArguments(request);
    args.push(
      "contextual", "react", "--module-hash", request.reactiveModuleHash,
      "--operation", request.operationName, "--consumer-name", request.consumerName,
      "--reaction", expectSymbol(reaction.name),
      "--causation-token", expectBoundedLowerHex(reaction.causationToken, 2_048),
      "--command-name", command.commandName,
      "--expected-version", String(command.contractVersion),
    );
    addReactiveParameters(args, request.parameters, request.parameterSchema);
    const envelope = await this.invoke(args, input, command.decodeError, "input");
    return decodeTypedCommandResult(exactObject(envelope.result), command);
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

  private async mutateContextualItem<P>(
    action: "ack" | "nack",
    request: ReactiveConsumerRequest<P>,
    item: ContextualWorkItem<unknown>,
    retryDelayMs = 0,
  ): Promise<ReactiveEventMutationResult> {
    validateReactiveRequest(request);
    const delay = boundedInteger(retryDelayMs, 0, 3_600_000);
    const args = this.reactiveArguments(request);
    args.push(
      "contextual", action, "--module-hash", request.reactiveModuleHash,
      "--operation", request.operationName, "--consumer-name", request.consumerName,
      "--event-id", expectBoundedString(item.delivery.eventId, 64),
      "--lease-token", expectHash(item.delivery.leaseToken),
      "--history-incarnation", item.delivery.historyIncarnation.toString(),
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
    inputKind?: "input" | "parameters",
  ): Promise<Record<string, unknown>> {
    const directory = await mkdtemp(join(tmpdir(), "riffdb-typescript-"));
    const inputPath = join(directory, "input.json");
    try {
      await writeFile(inputPath, `${JSON.stringify(input)}\n`, { encoding: "utf8", mode: 0o600 });
      const commandArgs = [...args];
      const selectedInput = inputKind ?? (commandArgs.includes("command") ? "input" : "parameters");
      commandArgs.push(selectedInput === "input" ? "--input" : "--parameters", inputPath);
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

/**
 * Generated-facade adapter for one retained Rust driver-host session.
 *
 * The adapter owns only checked value assembly and result decoding. Remote
 * trust, credentials, retries, pooling, and uncertainty remain in Rust.
 */
export class DriverGeneratedApplicationTransport {
  public constructor(private readonly driver: DriverApplicationTransport) {}

  public async executeNamedQuery<P, R>(
    request: NamedQueryRequest<P, R>,
    options: QueryOptions = {},
  ): Promise<TypedQueryResult<R>> {
    validateIdentity(request);
    const operation = requireDriverOperation(request.driverOperation);
    const result = await this.driver.invoke(
      operation,
      encodeDriverRecord(request.parameters, request.parameterSchema),
      {
        ...(options.cursor === undefined ? {} : { cursor: options.cursor }),
        ...(options.readAfterCommit === undefined ? {} : { readAfterCommit: options.readAfterCommit }),
        ...(options.consistency === undefined ? {} : { queryConsistency: options.consistency }),
        ...(request.compactDecoder === undefined ? {} : { acceptCompactResult: true }),
        ...(request.packedDecoder === undefined ? {} : { acceptPackedResult: true }),
      },
    );
    if (result.applicationHead === undefined) throw new Error("RiffDB driver omitted the query frontier");
    let value: R;
    if (result.packed !== undefined) {
      if (request.packedDecoder === undefined || result.value !== undefined || result.compact !== undefined) throw new Error("RiffDB driver returned an unexpected packed result");
      value = request.packedDecoder(result.packed);
    } else if (result.compact !== undefined) {
      if (request.compactDecoder === undefined || result.value !== undefined) throw new Error("RiffDB driver returned an unexpected compact result");
      value = request.compactDecoder(result.compact);
    } else {
      const raw = requireDriverValue(result);
      const outcome = driverOutcome(raw);
      const resultSchema = request.resultSchemas[outcome];
      if (resultSchema === undefined) throw new Error("RiffDB driver returned an unknown query outcome");
      value = decodeDriverValue(raw, {
        kind: "record",
        fields: [
          { name: "outcome", schema: { kind: "enum" } },
          ...recordFields(resultSchema),
        ],
      }) as R;
    }
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

  public async executeCommand<I, R>(
    request: CommandRequest<I, R>,
    attemptBudget: number,
  ): Promise<TypedCommandResult<R>> {
    validateIdentity(request);
    const operation = requireDriverOperation(request.driverOperation);
    const result = await this.driver.invoke(
      operation,
      encodeDriverRecord(request.input, request.inputSchema),
      { maximumAttempts: boundedInteger(attemptBudget, 1, 10) },
    );
    return decodeDriverCommandResult(request, requireDriverValue(result), result.applicationHead, result.cursor, result.replayed);
  }

  public async executeVectorInspection<P, R>(
    request: VectorInspectionRequest<P, R>,
    options: VectorInspectionOptions = {},
  ): Promise<TypedVectorInspectionResult<R>> {
    validateIdentity(request);
    const operation = requireDriverOperation(request.driverOperation);
    const result = await this.driver.invoke(
      operation,
      {
        partition: encodeDriverValue(request.partition, request.partitionSchema),
        limit: { type: "u64", value: BigInt(boundedInteger(request.limit, 1, 500)).toString() },
      },
      { ...(options.cursor === undefined ? {} : { cursor: options.cursor }) },
    );
    const value = decodeDriverVectorInspection(requireDriverValue(result)) as R;
    return {
      value,
      ...(result.applicationHead === undefined ? {} : { applicationHead: result.applicationHead }),
      ...(result.cursor === undefined ? {} : { nextCursor: result.cursor }),
    };
  }

  public async executeCommandBatch<I, R>(
    request: CommandRequest<I, R>,
    inputs: ReadonlyArray<I>,
    concurrency: number,
    checkpoint: number,
    attemptBudget: number,
  ): Promise<{ readonly items: ReadonlyArray<{ readonly index: number; readonly result?: TypedCommandResult<R>; readonly error?: DriverApplicationError }>; readonly checkpoint: number }> {
    validateIdentity(request);
    const operation = requireDriverOperation(request.driverOperation);
    const result = await this.driver.batch(
      operation,
      inputs.map((input) => encodeDriverRecord(input, request.inputSchema)),
      concurrency,
      checkpoint,
      { maximumAttempts: boundedInteger(attemptBudget, 1, 10) },
    );
    return {
      checkpoint: result.checkpoint,
      items: result.items.map((item) => item.result === undefined
        ? { index: item.index, ...(item.error === undefined ? {} : { error: item.error }) }
        : {
            index: item.index,
            result: decodeDriverCommandResult(
              request,
              item.result.value,
              item.result.commitSequence,
              item.result.outcomeUri,
              item.result.replayed,
            ),
          }),
    };
  }

  public async *consumeEventStream<P, E>(
    request: ReactiveConsumerRequest<P>,
    options: ReactiveConsumerOptions = {},
  ): AsyncIterable<ReactiveConsumerBatch<E>> {
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
      const result = await this.driver.invoke(
        requireReactiveDriverOperation(request, "next"),
        input,
        { deadlineMillis: Math.max(1, maximumWaitMs), ...(options.signal === undefined ? {} : { signal: options.signal }) },
      );
      yield decodeDriverEventBatch<E>(requireDriverValue(result));
    }
  }

  public async acknowledgeEvent<P>(
    request: ReactiveConsumerRequest<P>,
    delivery: ReactiveEventDelivery<unknown>,
  ): Promise<ReactiveEventMutationResult> {
    return this.mutateDriverLease("ack", request, delivery);
  }

  public async negativeAcknowledgeEvent<P>(
    request: ReactiveConsumerRequest<P>,
    delivery: ReactiveEventDelivery<unknown>,
    retryDelayMs = 0,
  ): Promise<ReactiveEventMutationResult> {
    const delay = boundedInteger(retryDelayMs, 0, 300_000);
    return this.mutateDriverLease("nack", request, delivery, BigInt(delay) * 1_000_000n);
  }

  public async seekEventConsumer<P>(
    request: ReactiveConsumerRequest<P>,
    checkpoint: string,
  ): Promise<ReactiveEventMutationResult> {
    const input = reactiveDriverInput(request);
    input.checkpoint = { type: "string", value: expectBoundedString(checkpoint, 256) };
    const result = await this.driver.invoke(requireReactiveDriverOperation(request, "seek"), input);
    return decodeDriverMutationResult(requireDriverValue(result));
  }

  public async eventConsumerStatus<P>(
    request: ReactiveConsumerRequest<P>,
  ): Promise<ReactiveConsumerStatus | undefined> {
    const result = await this.driver.invoke(
      requireReactiveDriverOperation(request, "status"),
      reactiveDriverInput(request),
    );
    const value = requireDriverValue(result);
    return value.type === "null" ? undefined : decodeDriverConsumerStatus(value);
  }

  public async consumeContextualSubscription<P, E>(
    request: ReactiveConsumerRequest<P>,
    maximumWaitMs = 30_000,
    signal?: AbortSignal,
  ): Promise<ContextualBatch<E>> {
    const wait = boundedInteger(maximumWaitMs, 0, 30_000);
    const result = await this.driver.invoke(
      requireReactiveDriverOperation(request, "next"),
      reactiveDriverInput(request),
      { deadlineMillis: Math.max(1, wait), ...(signal === undefined ? {} : { signal }) },
    );
    return decodeDriverContextualBatch<E>(requireDriverValue(result));
  }

  public async acknowledgeContextualItem<P>(
    request: ReactiveConsumerRequest<P>,
    item: ContextualWorkItem<unknown>,
  ): Promise<ReactiveEventMutationResult> {
    return this.mutateDriverLease("ack", request, item.delivery);
  }

  public async negativeAcknowledgeContextualItem<P>(
    request: ReactiveConsumerRequest<P>,
    item: ContextualWorkItem<unknown>,
    retryDelayMs = 0,
  ): Promise<ReactiveEventMutationResult> {
    const delay = boundedInteger(retryDelayMs, 0, 300_000);
    return this.mutateDriverLease("nack", request, item.delivery, BigInt(delay) * 1_000_000n);
  }

  public async contextualSubscriptionStatus<P>(
    request: ReactiveConsumerRequest<P>,
  ): Promise<ReactiveConsumerStatus | undefined> {
    return this.eventConsumerStatus(request);
  }

  public async executeContextualReaction<P, I, R>(
    request: ReactiveConsumerRequest<P>,
    reaction: ContextualReaction,
    command: CommandRequest<I, R>,
  ): Promise<TypedCommandResult<R>> {
    validateIdentity(command);
    if (reaction.commandName !== command.commandName) {
      throw new Error("contextual reaction command identity mismatch");
    }
    const action = `react_${snakeDriverAction(reaction.name)}`;
    const input = reactiveDriverInput(request);
    input.causation_token = { type: "string", value: expectBoundedString(reaction.causationToken, 16_384) };
    input.input = encodeDriverValue(command.input, command.inputSchema);
    const result = await this.driver.invoke(requireReactiveDriverOperation(request, action), input);
    return decodeDriverCommandResult(
      command,
      requireDriverValue(result),
      result.applicationHead,
      result.cursor,
      result.replayed,
    );
  }

  public async *watchNamedQuery<P, T>(request: {
    readonly driverOperations?: Readonly<Record<string, DriverOperation>>;
    readonly reactiveModuleHash: string;
    readonly operationName: string;
    readonly parameters: P;
    readonly parameterSchema: ApplicationValueSchema;
    readonly cursor?: string;
    readonly signal?: AbortSignal;
  }): AsyncIterable<LiveQueryUpdate<T>> {
    validateDriverReactiveRequest(request);
    let cursor = request.cursor;
    while (request.signal?.aborted !== true) {
      const input = reactiveDriverInput(request);
      if (cursor !== undefined) input.cursor = { type: "string", value: expectBoundedString(cursor, 16_384) };
      const result = await this.driver.invoke(
        requireReactiveDriverOperation(request, "watch"),
        input,
        { ...(cursor === undefined ? {} : { cursor }), ...(request.signal === undefined ? {} : { signal: request.signal }) },
      );
      const update = decodeDriverLiveUpdate<T>(requireDriverValue(result), result.applicationHead, result.cursor);
      yield update;
      if (update.type === "terminal") return;
      cursor = update.cursor;
    }
  }

  private async mutateDriverLease<P>(
    action: "ack" | "nack",
    request: ReactiveConsumerRequest<P>,
    delivery: ReactiveEventDelivery<unknown>,
    retryDelayNanos?: bigint,
  ): Promise<ReactiveEventMutationResult> {
    const input = reactiveDriverInput(request);
    input.event_id = { type: "string", value: expectBoundedString(delivery.eventId, 256) };
    input.lease_token = { type: "string", value: expectBoundedString(delivery.leaseToken, 16_384) };
    input.history_incarnation = driverU64(delivery.historyIncarnation);
    if (action === "nack") input.retry_delay_nanos = driverU64(retryDelayNanos ?? 0n);
    const result = await this.driver.invoke(requireReactiveDriverOperation(request, action), input);
    return decodeDriverMutationResult(requireDriverValue(result));
  }
}

function validateDriverReactiveRequest(value: {
  readonly driverOperations?: Readonly<Record<string, DriverOperation>>;
  readonly reactiveModuleHash: string;
  readonly operationName: string;
  readonly parameters: unknown;
  readonly parameterSchema: ApplicationValueSchema;
  readonly consumerName?: string;
}): void {
  if (!HASH.test(value.reactiveModuleHash) || !SYMBOL.test(value.operationName)) {
    throw new Error("invalid generated reactive operation identity");
  }
  if (value.parameterSchema.kind !== "record") {
    throw new Error("invalid generated reactive parameter schema");
  }
  if (value.consumerName !== undefined) expectSymbol(value.consumerName);
  if (value.driverOperations === undefined || Object.keys(value.driverOperations).length < 1
      || Object.keys(value.driverOperations).length > 16) {
    throw new Error("generated reactive binding has no driver operation identities");
  }
  for (const [action, operation] of Object.entries(value.driverOperations)) {
    if (!/^[a-z][a-z0-9_]{0,127}$/.test(action)) throw new Error("invalid generated reactive action");
    requireDriverOperation(operation);
  }
}

function requireReactiveDriverOperation(
  request: { readonly driverOperations?: Readonly<Record<string, DriverOperation>> },
  action: string,
): DriverOperation {
  const operation = request.driverOperations?.[action];
  if (operation === undefined) throw new Error("generated reactive binding has no driver action identity");
  return requireDriverOperation(operation);
}

function reactiveDriverInput(request: {
  readonly parameters: unknown;
  readonly parameterSchema: ApplicationValueSchema;
  readonly consumerName?: string;
}): Record<string, DriverValue> {
  const input: Record<string, DriverValue> = {
    parameters: encodeDriverValue(request.parameters, request.parameterSchema),
  };
  if (request.consumerName !== undefined) {
    input.consumer_name = { type: "string", value: expectSymbol(request.consumerName) };
  }
  return input;
}

function driverU64(value: bigint): DriverValue {
  if (value < 0n || value > (1n << 64n) - 1n) throw new Error("invalid generated RiffDB u64 value");
  return { type: "u64", value: value.toString() };
}

function decodeDriverMutationResult(value: DriverValue): ReactiveEventMutationResult {
  if (value.type !== "enum" || ![
    "applied", "state_changed", "not_found", "outstanding_lease", "stale_lease", "lease_expired",
  ].includes(value.value)) {
    throw new Error("invalid RiffDB driver mutation result");
  }
  return value.value as ReactiveEventMutationResult;
}

function decodeDriverConsumerStatus(value: DriverValue): ReactiveConsumerStatus {
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

function decodeDriverEventBatch<E>(value: DriverValue): ReactiveConsumerBatch<E> {
  const batch = driverRecord(value);
  const events = driverList(requiredDriverField(batch, "events")).map(decodeDriverDelivery<E>);
  const timedOut = requiredDriverField(batch, "wait_timed_out");
  if (timedOut.type !== "bool") throw new Error("invalid RiffDB driver event batch");
  return {
    events,
    waitTimedOut: timedOut.value,
    status: decodeDriverConsumerStatus(requiredDriverField(batch, "status")),
  };
}

function decodeDriverDelivery<E>(value: DriverValue): ReactiveEventDelivery<E> {
  const delivery = driverRecord(value);
  const eventName = driverEnum(requiredDriverField(delivery, "event_name"));
  const fields = driverRecord(requiredDriverField(delivery, "fields"));
  const expiration = requiredDriverField(delivery, "expires_at");
  if (expiration.type !== "timestamp") throw new Error("invalid RiffDB driver event lease");
  return {
    eventId: driverString(requiredDriverField(delivery, "event_id"), 256),
    event: { type: eventName, ...decodeDriverDynamicRecord(fields) } as E,
    attempt: driverBoundedU32(requiredDriverField(delivery, "attempt")),
    leaseToken: driverString(requiredDriverField(delivery, "lease_token"), 16_384),
    expiresAt: `${expiration.value.seconds}.${String(expiration.value.nanos).padStart(9, "0")}`,
    historyIncarnation: driverPositiveU64(requiredDriverField(delivery, "history_incarnation")),
  };
}

function decodeDriverContextualBatch<E>(value: DriverValue): ContextualBatch<E> {
  const batch = driverRecord(value);
  const timedOut = requiredDriverField(batch, "wait_timed_out");
  if (timedOut.type !== "bool") throw new Error("invalid RiffDB driver contextual batch");
  return {
    items: driverList(requiredDriverField(batch, "items")).map((raw): ContextualWorkItem<E> => {
      const item = driverRecord(raw);
      return {
        delivery: decodeDriverDelivery<E>(requiredDriverField(item, "delivery")),
        contextHead: driverPositiveU64(requiredDriverField(item, "context_head")),
        hydrations: driverList(requiredDriverField(item, "hydrations")).map((rawHydration) => {
          const hydration = driverRecord(rawHydration);
          const fields: Record<string, unknown> = {};
          for (const [name, field] of Object.entries(hydration)) {
            if (name !== "name" && name !== "outcome") fields[name] = decodeDriverDynamic(field);
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

function decodeDriverLiveUpdate<T>(
  value: DriverValue,
  applicationHead: bigint | undefined,
  responseCursor: string | undefined,
): LiveQueryUpdate<T> {
  const update = driverRecord(value);
  const kind = driverEnum(requiredDriverField(update, "kind"));
  if (kind === "terminal") {
    const reason = driverEnum(requiredDriverField(update, "reason"));
    const frontier = requiredDriverField(update, "last_frontier");
    if (frontier.type === "null") return { type: "terminal", reason };
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
    return { type: "snapshot", value: decodeDriverDynamic(requiredDriverField(update, "result")) as T, cursor, applicationHead: head, historyIncarnation: history };
  }
  if (kind === "reset") {
    return { type: "reset", reason: driverEnum(requiredDriverField(update, "reason")), value: decodeDriverDynamic(requiredDriverField(update, "result")) as T, cursor, applicationHead: head, historyIncarnation: history };
  }
  if (kind === "checkpoint") {
    return { type: "checkpoint", cursor, applicationHead: head, historyIncarnation: history };
  }
  if (kind !== "patch") throw new Error("invalid RiffDB driver live update");
  return {
    type: "patch",
    resultField: driverString(requiredDriverField(update, "result_field"), 256),
    operations: driverList(requiredDriverField(update, "operations")).map(decodeDriverPatchOperation),
    cursor,
    applicationHead: head,
    historyIncarnation: history,
  };
}

function decodeDriverPatchOperation(value: DriverValue): LiveQueryPatchOperation {
  const operation = driverRecord(value);
  const kind = driverEnum(requiredDriverField(operation, "operation"));
  if (kind === "insert" || kind === "replace") {
    const record = decodeDriverDynamic(requiredDriverField(operation, "record"));
    if (!isPlainRecord(record)) throw new Error("invalid RiffDB driver live record");
    return { type: kind, index: driverBoundedU32(requiredDriverField(operation, "index")), record };
  }
  const key = decodeDriverDynamic(requiredDriverField(operation, "key"));
  if (!isPlainRecord(key)) throw new Error("invalid RiffDB driver live key");
  if (kind === "remove") {
    return { type: "remove", index: driverBoundedU32(requiredDriverField(operation, "index")), key };
  }
  if (kind === "move") {
    return { type: "move", from: driverBoundedU32(requiredDriverField(operation, "from")), to: driverBoundedU32(requiredDriverField(operation, "to")), key };
  }
  throw new Error("invalid RiffDB driver live patch operation");
}

function decodeDriverDynamic(value: DriverValue): unknown {
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
      if (value.value.precision === undefined) throw new Error("invalid RiffDB driver decimal");
      return { coefficientTwosComplement: Uint8Array.from(Buffer.from(value.value.coefficient, "base64")), scale: value.value.scale, precision: value.value.precision };
    }
    case "money": {
      if (value.value.amount.precision === undefined) throw new Error("invalid RiffDB driver money");
      return { currency: value.value.currency, amount: { coefficientTwosComplement: Uint8Array.from(Buffer.from(value.value.amount.coefficient, "base64")), scale: value.value.amount.scale, precision: value.value.amount.precision } };
    }
    case "list": return value.value.map(decodeDriverDynamic);
    case "record": return decodeDriverDynamicRecord(value.value);
  }
}

function decodeDriverDynamicRecord(value: Readonly<Record<string, DriverValue>>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).map(([name, field]) => [name, decodeDriverDynamic(field)]));
}

function driverRecord(value: DriverValue): Readonly<Record<string, DriverValue>> {
  if (value.type !== "record") throw new Error("invalid RiffDB driver record");
  return value.value;
}

function driverList(value: DriverValue): ReadonlyArray<DriverValue> {
  if (value.type !== "list") throw new Error("invalid RiffDB driver list");
  return value.value;
}

function requiredDriverField(
  value: Readonly<Record<string, DriverValue>>,
  name: string,
): DriverValue {
  const field = value[name];
  if (field === undefined) throw new Error("RiffDB driver result is missing a field");
  return field;
}

function driverPositiveU64(value: DriverValue): bigint {
  if (value.type !== "u64") throw new Error("invalid RiffDB driver u64");
  const parsed = BigInt(value.value);
  if (parsed < 1n || parsed > (1n << 64n) - 1n) throw new Error("invalid RiffDB driver u64");
  return parsed;
}

function driverBoundedU32(value: DriverValue): number {
  if (value.type !== "u64") throw new Error("invalid RiffDB driver u32");
  const parsed = BigInt(value.value);
  if (parsed < 0n) throw new Error("invalid RiffDB driver u32");
  if (parsed > 4_294_967_295n) throw new Error("invalid RiffDB driver u32");
  return Number(parsed);
}

function driverString(value: DriverValue, maximum: number): string {
  if (value.type !== "string") throw new Error("invalid RiffDB driver string");
  return expectBoundedString(value.value, maximum);
}

function driverEnum(value: DriverValue): string {
  if (value.type !== "enum") throw new Error("invalid RiffDB driver enum");
  return expectSymbol(value.value);
}

function snakeDriverAction(value: string): string {
  const result = value.replace(/([a-z0-9])([A-Z])/g, "$1_$2").replace(/[^A-Za-z0-9]+/g, "_").toLowerCase();
  if (!/^[a-z][a-z0-9_]{0,127}$/.test(result)) throw new Error("invalid contextual reaction identity");
  return result;
}

function isPlainRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value) && !(value instanceof Uint8Array);
}

function requireDriverOperation(operation: DriverOperation | undefined): DriverOperation {
  if (operation === undefined) throw new Error("generated RiffDB binding has no driver operation identity");
  return operation;
}

function encodeDriverRecord(value: unknown, schema: ApplicationValueSchema): Readonly<Record<string, DriverValue>> {
  if (schema.kind !== "record") throw new Error("invalid generated RiffDB record schema");
  const encoded = encodeDriverValue(value, schema);
  if (encoded.type !== "record") throw new Error("invalid generated RiffDB record");
  return encoded.value;
}

function encodeDriverValue(value: unknown, schema: ApplicationValueSchema, budgetPath?: InputBudgetPath): DriverValue {
  if (schema.kind === "optional") {
    return value === null || value === undefined ? { type: "null" } : encodeDriverValue(value, schema.value, budgetPath);
  }
  if (schema.kind === "list") {
    if (!Array.isArray(value)
        || value.length < (schema.minimum ?? 0)
        || value.length > (schema.maximum ?? 4_096)) {
      if (budgetPath !== undefined) throw new InputBudgetError("collection_count", budgetPath);
      throw new Error("invalid generated RiffDB list input");
    }
    const encoded = value.map((item, index) => encodeDriverValue(item, schema.value, budgetPath === undefined ? undefined : { ...budgetPath, index }));
    if (schema.aggregateCanonicalElementBytes !== undefined) {
      let aggregate = 0;
      for (const item of value) {
        aggregate += canonicalValueEncodedLength(item, schema.value);
        if (!Number.isSafeInteger(aggregate) || aggregate > schema.aggregateCanonicalElementBytes) {
          if (budgetPath !== undefined) throw new InputBudgetError("aggregate_canonical_element_bytes", budgetPath);
          throw new Error("invalid generated RiffDB input");
        }
      }
    }
    return { type: "list", value: encoded };
  }
  if (schema.kind === "record") {
    const input = exactObject(value);
    const fields = new Map(schema.fields.map((field) => [field.name, field.schema]));
    if (Object.keys(input).some((name) => !fields.has(name))) throw new Error("invalid generated RiffDB record input");
    const output: Record<string, DriverValue> = {};
    for (const [name, fieldSchema] of fields) {
      const fieldValue = input[name];
      if (fieldValue === undefined && fieldSchema.kind === "optional") continue;
      if (fieldValue === undefined) throw new Error("generated RiffDB input is missing a field");
      const childPath = budgetPath === undefined
        ? (fieldSchema.kind === "list" && fieldSchema.aggregateCanonicalElementBytes !== undefined
          ? { collection: name }
          : undefined)
        : { ...budgetPath, leaf: name };
      output[name] = encodeDriverValue(fieldValue, fieldSchema, childPath);
    }
    return { type: "record", value: output };
  }
  switch (schema.kind) {
    case "bool":
      if (typeof value !== "boolean") throw new Error("invalid generated RiffDB bool input");
      return { type: "bool", value };
    case "i64":
      if (typeof value !== "bigint" || value < -(1n << 63n) || value > (1n << 63n) - 1n) throw new Error("invalid generated RiffDB i64 input");
      return { type: "i64", value: value.toString() };
    case "u64":
      if (typeof value !== "bigint" || value < 0n || value > (1n << 64n) - 1n) throw new Error("invalid generated RiffDB u64 input");
      return { type: "u64", value: value.toString() };
    case "string": {
      const string = expectBoundedString(value, 262_144);
      if (schema.maximumBytes !== undefined && new TextEncoder().encode(string).length > schema.maximumBytes && budgetPath !== undefined) {
        throw new InputBudgetError("individual_value_bytes", budgetPath);
      }
      return { type: "string", value: string };
    }
    case "cursor": return { type: "string", value: expectBoundedString(value, 262_144) };
    case "uuid":
      if (typeof value !== "string" || !UUID.test(value)) throw new Error("invalid generated RiffDB UUID input");
      return { type: "uuid", value };
    case "enum": return { type: "enum", value: expectSymbol(value) };
    case "bytes":
      if (!(value instanceof Uint8Array) || value.byteLength > 1_048_576) throw new Error("invalid generated RiffDB bytes input");
      if (schema.maximumBytes !== undefined && value.byteLength > schema.maximumBytes && budgetPath !== undefined) {
        throw new InputBudgetError("individual_value_bytes", budgetPath);
      }
      return { type: "bytes", value: Buffer.from(value).toString("base64") };
    case "date":
      if (!Number.isInteger(value) || (value as number) < -2_147_483_648 || (value as number) > 2_147_483_647) throw new Error("invalid generated RiffDB date input");
      return { type: "date", value: String(value) };
    case "timestamp": {
      const timestamp = exactObject(value);
      if (typeof timestamp.seconds !== "bigint" || timestamp.seconds < -(1n << 63n) || timestamp.seconds > (1n << 63n) - 1n) throw new Error("invalid generated RiffDB timestamp input");
      return { type: "timestamp", value: { seconds: timestamp.seconds.toString(), nanos: boundedInteger(timestamp.nanos, 0, 999_999_999) } };
    }
    case "decimal": return { type: "decimal", value: encodeDriverDecimal(value, schema) };
    case "money": {
      const money = exactObject(value);
      const currency = expectCurrency(money.currency);
      if (currency !== schema.currency) throw new Error("generated RiffDB money currency does not match");
      return { type: "money", value: { currency, amount: encodeDriverDecimal(money.amount, schema) } };
    }
    case "vector": return {
      type: "vector",
      value: { component_bits: encodeVectorBits(value, schema.dimension) },
    };
    case "limit": return { type: "u64", value: String(boundedInteger(value, 1, 500)) };
  }
}

function encodeDriverDecimal(value: unknown, schema: { readonly precision?: number; readonly scale?: number }): { readonly coefficient: string; readonly precision?: number; readonly scale: number } {
  const decimal = encodeDecimal(value);
  if (schema.precision === undefined || schema.scale === undefined || decimal.scale !== schema.scale
      || (decimal.precision !== undefined && decimal.precision !== schema.precision)) {
    throw new Error("generated RiffDB decimal does not match its schema");
  }
  return { coefficient: decimal.coefficient_twos_complement, precision: schema.precision, scale: schema.scale };
}

function driverOutcome(value: DriverValue): string {
  if (value.type !== "record") throw new Error("RiffDB driver returned a non-record application result");
  const outcome = value.value.outcome;
  if (outcome?.type !== "enum") throw new Error("RiffDB driver returned no declared outcome");
  return expectSymbol(outcome.value);
}

function requireDriverValue(result: {
  readonly value?: DriverValue;
  readonly compact?: unknown;
}): DriverValue {
  if (result.value === undefined || result.compact !== undefined) {
    throw new Error("RiffDB driver returned an unexpected compact result");
  }
  return result.value;
}

function decodeDriverVectorInspection(value: DriverValue): unknown {
  if (value.type !== "record") throw new Error("RiffDB driver returned invalid vector inspection");
  const kindValue = value.value.kind;
  if (kindValue?.type !== "enum") throw new Error("RiffDB driver returned invalid vector inspection");
  const u64 = (name: string, optional = false): bigint | null => {
    const field = value.value[name];
    if (optional && field?.type === "null") return null;
    if (field?.type !== "u64" || !/^(?:0|[1-9][0-9]*)$/.test(field.value)) throw new Error("RiffDB driver returned invalid vector inspection");
    return BigInt(field.value);
  };
  const items = (): ReadonlyArray<Readonly<Record<string, DriverValue>>> => {
    const field = value.value.items;
    if (field?.type !== "list" || field.value.length > 500 || field.value.some((item) => item.type !== "record")) {
      throw new Error("RiffDB driver returned invalid vector inspection");
    }
    return field.value.map((item) => (item as Extract<DriverValue, { type: "record" }>).value);
  };
  switch (kindValue.value) {
    case "staleness_summary": {
      const breached = value.value.slo_breached;
      if (breached?.type !== "bool") throw new Error("RiffDB driver returned invalid vector inspection");
      return { kind: "staleness_summary", totalEntities: u64("total_entities"), staleCount: u64("stale_count"), staleEntityCountThreshold: u64("stale_entity_count_threshold"), sloBreached: breached.value };
    }
    case "model_version_summary":
      return { kind: "model_version_summary", currentCount: u64("current_count"), outdatedCount: u64("outdated_count") };
    case "stale_entities":
      return {
        kind: "stale_entities",
        items: items().map((item) => ({
          entityKey: decodeDriverBytes(item.entity_key),
          newestSourceWrite: decodeDriverU64(item.newest_source_write),
          embeddingWrite: item.embedding_write?.type === "null" ? null : decodeDriverU64(item.embedding_write),
        })),
        observedFrontier: u64("observed_frontier", true),
      };
    case "outdated_model_entities":
      return {
        kind: "outdated_model_entities",
        items: items().map((item) => ({
          entityKey: decodeDriverBytes(item.entity_key),
          model: decodeDriverString(item.model),
          modelVersion: decodeDriverString(item.model_version),
          embeddingWrite: decodeDriverU64(item.embedding_write),
        })),
        observedFrontier: u64("observed_frontier", true),
      };
    default: throw new Error("RiffDB driver returned invalid vector inspection");
  }
}

function decodeDriverU64(value: DriverValue | undefined): bigint {
  if (value?.type !== "u64" || !/^(?:0|[1-9][0-9]*)$/.test(value.value)) throw new Error("RiffDB driver returned invalid vector inspection");
  return BigInt(value.value);
}

function decodeDriverString(value: DriverValue | undefined): string {
  if (value?.type !== "string") throw new Error("RiffDB driver returned invalid vector inspection");
  return expectBoundedString(value.value, 256);
}

function decodeDriverBytes(value: DriverValue | undefined): Uint8Array {
  if (value?.type !== "bytes") throw new Error("RiffDB driver returned invalid vector inspection");
  const bytes = Uint8Array.from(Buffer.from(value.value, "base64"));
  if (bytes.byteLength < 1 || bytes.byteLength > 1_048_576) throw new Error("RiffDB driver returned invalid vector inspection");
  return bytes;
}

function decodePlainVectorInspection(value: Record<string, unknown>): unknown {
  switch (value.kind) {
    case "staleness_summary":
      return { kind: value.kind, totalEntities: positiveOrZeroBigInt(value.total_entities), staleCount: positiveOrZeroBigInt(value.stale_count), staleEntityCountThreshold: positiveOrZeroBigInt(value.stale_entity_count_threshold), sloBreached: value.slo_breached === true };
    case "model_version_summary":
      return { kind: value.kind, currentCount: positiveOrZeroBigInt(value.current_count), outdatedCount: positiveOrZeroBigInt(value.outdated_count) };
    case "stale_entities":
      return { kind: value.kind, items: boundedPlainInspectionItems(value.items).map((item) => ({ entityKey: lowerHexBytes(item.entity_key), newestSourceWrite: positiveBigInt(item.newest_source_write), embeddingWrite: item.embedding_write === null ? null : positiveBigInt(item.embedding_write) })), observedFrontier: value.observed_frontier === null ? null : positiveBigInt(value.observed_frontier) };
    case "outdated_model_entities":
      return { kind: value.kind, items: boundedPlainInspectionItems(value.items).map((item) => ({ entityKey: lowerHexBytes(item.entity_key), model: expectBoundedString(item.model, 256), modelVersion: expectBoundedString(item.model_version, 256), embeddingWrite: positiveBigInt(item.embedding_write) })), observedFrontier: value.observed_frontier === null ? null : positiveBigInt(value.observed_frontier) };
    default: throw new Error("RiffDB CLI returned invalid vector inspection");
  }
}

function boundedPlainInspectionItems(value: unknown): ReadonlyArray<Record<string, unknown>> {
  if (!Array.isArray(value) || value.length > 500) throw new Error("RiffDB CLI returned invalid vector inspection");
  return value.map(exactObject);
}

function positiveOrZeroBigInt(value: unknown): bigint {
  if (typeof value !== "string" || !/^(?:0|[1-9][0-9]*)$/.test(value)) throw new Error("invalid nonnegative integer");
  return BigInt(value);
}

function lowerHexBytes(value: unknown): Uint8Array {
  const text = expectBoundedString(value, 2_097_152);
  if (text.length < 2 || text.length % 2 !== 0 || !/^[0-9a-f]+$/.test(text)) throw new Error("invalid lower-hex bytes");
  return Uint8Array.from(text.match(/../g)!.map((byte) => Number.parseInt(byte, 16)));
}

function decodeDriverCommandResult<I, R>(
  request: CommandRequest<I, R>,
  value: DriverValue,
  commitSequence: bigint | undefined,
  outcomeUri: string | undefined,
  replayed: boolean,
): TypedCommandResult<R> {
  const outcome = driverOutcome(value);
  const schema = request.outcomeSchemas[outcome];
  if (schema === undefined) throw new Error("RiffDB driver returned an unknown command outcome");
  return {
    outcome: decodeDriverValue(value, {
      kind: "record",
      fields: [{ name: "outcome", schema: { kind: "enum" } }, ...recordFields(schema)],
    }) as R,
    contractVersion: request.contractVersion,
    planHash: request.planHash,
    replayed,
    ...(commitSequence === undefined ? {} : { commitSequence }),
    ...(outcomeUri === undefined ? {} : { outcomeUri }),
  };
}

function decodeDriverValue(value: DriverValue, schema: ApplicationValueSchema): unknown {
  if (schema.kind === "optional") return value.type === "null" ? null : decodeDriverValue(value, schema.value);
  if (schema.kind === "list") {
    if (value.type !== "list"
        || value.value.length < (schema.minimum ?? 0)
        || value.value.length > (schema.maximum ?? 4_096)) {
      throw new Error("invalid RiffDB driver list result");
    }
    return value.value.map((item) => decodeDriverValue(item, schema.value));
  }
  if (schema.kind === "record") {
    if (value.type !== "record") throw new Error("invalid RiffDB driver record result");
    const allowed = new Set(schema.fields.map((field) => field.name));
    if (Object.keys(value.value).some((name) => !allowed.has(name))) throw new Error("invalid RiffDB driver record result");
    const output: Record<string, unknown> = {};
    for (const field of schema.fields) {
      const fieldValue = value.value[field.name];
      if (fieldValue === undefined && field.schema.kind === "optional") { output[field.name] = null; continue; }
      if (fieldValue === undefined) throw new Error("invalid RiffDB driver record result");
      output[field.name] = decodeDriverValue(fieldValue, field.schema);
    }
    return output;
  }
  switch (schema.kind) {
    case "bool": if (value.type === "bool") return value.value; break;
    case "i64": if (value.type === "i64") { const parsed = BigInt(value.value); if (parsed >= -(1n << 63n) && parsed <= (1n << 63n) - 1n) return parsed; } break;
    case "u64": if (value.type === "u64") { const parsed = BigInt(value.value); if (parsed >= 0n && parsed <= (1n << 64n) - 1n) return parsed; } break;
    case "string":
    case "cursor": if (value.type === "string") return expectBoundedString(value.value, 262_144); break;
    case "uuid": if (value.type === "uuid" && UUID.test(value.value)) return value.value; break;
    case "enum": if (value.type === "enum") return expectSymbol(value.value); break;
    case "bytes": if (value.type === "bytes") return Uint8Array.from(Buffer.from(value.value, "base64")); break;
    case "date": if (value.type === "date") { const parsed = Number(value.value); if (Number.isInteger(parsed) && parsed >= -2_147_483_648 && parsed <= 2_147_483_647) return parsed; } break;
    case "timestamp": if (value.type === "timestamp") return { seconds: BigInt(value.value.seconds), nanos: boundedInteger(value.value.nanos, 0, 999_999_999) }; break;
    case "decimal": if (value.type === "decimal") return decodeDriverDecimal(value.value, schema); break;
    case "money": if (value.type === "money" && value.value.currency === schema.currency) return { currency: value.value.currency, amount: decodeDriverDecimal(value.value.amount, schema) }; break;
    case "vector": if (value.type === "vector") return decodeVectorBits(value.value.component_bits, schema.dimension); break;
    case "limit": if (value.type === "u64") return boundedInteger(Number(value.value), 1, 500); break;
  }
  throw new Error("RiffDB driver result does not match the generated schema");
}

function decodeDriverDecimal(value: { readonly coefficient: string; readonly scale: number; readonly precision?: number | null }, schema: { readonly precision?: number; readonly scale?: number }): ExactDecimalValue {
  if (schema.precision === undefined || schema.scale === undefined
      || (value.precision !== undefined && value.precision !== null && value.precision !== schema.precision)
      || value.scale !== schema.scale) throw new Error("RiffDB driver decimal does not match its schema");
  const coefficient = Uint8Array.from(Buffer.from(value.coefficient, "base64"));
  if (coefficient.byteLength < 1 || coefficient.byteLength > 16) throw new Error("RiffDB driver decimal coefficient is invalid");
  return { coefficientTwosComplement: coefficient, precision: schema.precision, scale: schema.scale };
}

function decodeTypedCommandResult<I, R>(
  result: Record<string, unknown>,
  request: CommandRequest<I, R>,
): TypedCommandResult<R> {
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
  if (result.commit_sequence === undefined) return typed;
  return {
    ...typed,
    commitSequence: positiveBigInt(result.commit_sequence),
    ...(result.outcome_uri === undefined
      ? {}
      : { outcomeUri: expectBoundedString(result.outcome_uri, 4096) }),
  };
}

function decodeContextualItem<E>(value: unknown): ContextualWorkItem<E> {
  const item = exactObject(value);
  return {
    delivery: decodeContextualDelivery<E>(item.delivery),
    contextHead: positiveBigInt(item.context_head),
    hydrations: expectArray(item.hydrations).map(decodeContextualHydration),
    availableReactions: expectArray(item.available_reactions).map((raw): ContextualReaction => {
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

function decodeContextualDelivery<E>(value: unknown): ReactiveEventDelivery<E> {
  const delivery = exactObject(value);
  const fields = Object.fromEntries(expectArray(delivery.fields).map((raw) => {
    const field = exactObject(raw);
    return [expectSymbol(field.name), decodeTagged(field.value)];
  }));
  const expiration = exactObject(delivery.expires_at);
  return {
    eventId: expectBoundedString(delivery.event_id, 64),
    event: { type: expectSymbol(delivery.event_name), ...fields } as E,
    attempt: positiveNumber(delivery.attempt),
    leaseToken: expectHash(delivery.lease_token),
    expiresAt: `${expectBoundedString(expiration.seconds, 32)}.${String(boundedInteger(expiration.nanos, 0, 999_999_999)).padStart(9, "0")}`,
    historyIncarnation: positiveBigInt(delivery.history_incarnation),
  };
}

function decodeContextualHydration(value: unknown): ContextualHydration {
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

function expectBoundedLowerHex(value: unknown, maximum: number): string {
  const text = expectBoundedString(value, maximum);
  if (text.length <= 64 || text.length % 2 !== 0 || !/^[0-9a-f]+$/.test(text)) {
    throw new Error("invalid opaque RiffDB token");
  }
  return text;
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

function encodeValue(value: unknown, schema: ApplicationValueSchema, budgetPath?: InputBudgetPath): unknown {
  if (schema.kind === "optional") {
    return value === null || value === undefined ? null : encodeValue(value, schema.value, budgetPath);
  }
  if (schema.kind === "list") {
    if (!Array.isArray(value)
        || (schema.minimum !== undefined && value.length < schema.minimum)
        || (schema.maximum !== undefined && value.length > schema.maximum)) {
      if (budgetPath !== undefined) throw new InputBudgetError("collection_count", budgetPath);
      throw new Error("invalid generated application input");
    }
    const encoded = value.map((item, index) => encodeValue(item, schema.value, budgetPath === undefined ? undefined : { ...budgetPath, index }));
    if (schema.aggregateCanonicalElementBytes !== undefined) {
      let aggregate = 0;
      for (const item of value) {
        aggregate += canonicalValueEncodedLength(item, schema.value);
        if (!Number.isSafeInteger(aggregate) || aggregate > schema.aggregateCanonicalElementBytes) {
          if (budgetPath !== undefined) throw new InputBudgetError("aggregate_canonical_element_bytes", budgetPath);
          throw new Error("invalid generated application input");
        }
      }
    }
    return encoded;
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
      const childPath = budgetPath === undefined
        ? (field.schema.kind === "list" && field.schema.aggregateCanonicalElementBytes !== undefined
          ? { collection: field.name }
          : undefined)
        : { ...budgetPath, leaf: field.name };
      output[field.name] = encodeValue(fieldValue, field.schema, childPath);
    }
    return output;
  }
  switch (schema.kind) {
    case "uuid":
      if (typeof value !== "string" || !UUID.test(value)) throw new Error("invalid UUID input");
      return { $uuid: value };
    case "enum":
      return { $enum: expectSymbol(value) };
    case "string": {
      const string = expectBoundedString(value, 262_144);
      if (schema.maximumBytes !== undefined && new TextEncoder().encode(string).length > schema.maximumBytes && budgetPath !== undefined) {
        throw new InputBudgetError("individual_value_bytes", budgetPath);
      }
      return string;
    }
    case "cursor": return expectBoundedString(value, 262_144);
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
      if (schema.maximumBytes !== undefined && value.byteLength > schema.maximumBytes && budgetPath !== undefined) {
        throw new InputBudgetError("individual_value_bytes", budgetPath);
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
    case "vector": return { $vector: encodeVector(value, schema.dimension) };
    case "limit":
      if (!Number.isInteger(value) || (value as number) < 1 || (value as number) > 500) {
        throw new Error("invalid limit input");
      }
      return value;
    default:
      throw new Error("generated application scalar is not supported by the CLI transport");
  }
}

function canonicalValueEncodedLength(value: unknown, schema: ApplicationValueSchema): number {
  if (schema.kind === "optional") {
    return value === null || value === undefined ? 2 : canonicalValueEncodedLength(value, schema.value);
  }
  if (schema.kind === "list") {
    if (!Array.isArray(value)) throw new Error("invalid generated application input");
    return value.reduce((total, item) => total + canonicalValueEncodedLength(item, schema.value), 6);
  }
  if (schema.kind === "record") {
    const input = exactObject(value);
    return schema.fields.reduce((total, field) => {
      const fieldValue = input[field.name];
      if (fieldValue === undefined && field.schema.kind !== "optional") {
        throw new Error("invalid generated application input");
      }
      return total + 4 + canonicalValueEncodedLength(fieldValue, field.schema);
    }, 6);
  }
  switch (schema.kind) {
    case "bool": return 3;
    case "i64":
    case "u64": return 10;
    case "decimal": return 20;
    case "money": return 23;
    case "string":
    case "cursor": return 6 + new TextEncoder().encode(expectBoundedString(value, 262_144)).byteLength;
    case "bytes": {
      if (!(value instanceof Uint8Array)) throw new Error("invalid generated application input");
      return 6 + value.byteLength;
    }
    case "timestamp": return 14;
    case "date": return 6;
    case "uuid": return 18;
    case "enum": return 10;
    case "vector": {
      if (!Array.isArray(value)) throw new Error("invalid generated application input");
      return 6 + value.length * 4;
    }
    default: throw new Error("invalid generated application input");
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
    case "vector": return decodeVector(input.components, undefined);
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
    if (!Array.isArray(value)
        || (schema.minimum !== undefined && value.length < schema.minimum)
        || (schema.maximum !== undefined && value.length > schema.maximum)) return fail();
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
      return normalizeDecodedDecimal(value, schema.precision, schema.scale);
    }
    case "money": {
      const input = exactObject(value);
      const currency = expectCurrency(input.currency);
      if (schema.currency !== undefined && currency !== schema.currency) return fail();
      return {
        currency,
        amount: normalizeDecodedDecimal(input.amount, schema.precision, schema.scale),
      };
    }
    case "vector": return decodeVector(value, schema.dimension);
    default: break;
  }
  return fail();
}

function normalizeDecodedDecimal(
  value: unknown,
  expectedPrecision: number | undefined,
  expectedScale: number | undefined,
): {
  readonly coefficientTwosComplement: Uint8Array;
  readonly scale: number;
  readonly precision?: number;
} {
  const input = exactObject(value);
  const encoded = encodeDecimal(value);
  if ((expectedPrecision !== undefined && encoded.precision !== undefined
        && encoded.precision !== expectedPrecision)
      || (expectedScale !== undefined && encoded.scale !== expectedScale)) return fail();
  const precision = expectedPrecision ?? encoded.precision;
  return {
    coefficientTwosComplement: input.coefficientTwosComplement as Uint8Array,
    scale: encoded.scale,
    ...(precision === undefined ? {} : { precision }),
  };
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
    case "vector": return {
      type: "vector",
      components: encodeVector(value, schema.dimension),
    };
    case "limit":
      return { type: "u64", value: String(boundedInteger(value, 1, 500)) };
    case "optional":
    case "list":
    case "record":
      throw new Error("invalid reactive parameter schema");
  }
}

function encodeVector(value: unknown, dimension: number): ReadonlyArray<number> {
  if (!Array.isArray(value) || !Number.isInteger(dimension) || dimension < 1
      || dimension > 4_096 || value.length !== dimension) return fail();
  return value.map((component) => {
    if (typeof component !== "number" || !Number.isFinite(component)) return fail();
    const rounded = Math.fround(component === 0 ? 0 : component);
    if (!Number.isFinite(rounded)) return fail();
    return rounded;
  });
}

function encodeVectorBits(value: unknown, dimension: number): ReadonlyArray<number> {
  const vector = encodeVector(value, dimension);
  const float = new Float32Array(1);
  const bits = new Uint32Array(float.buffer);
  return vector.map((component) => {
    float[0] = component;
    return bits[0]!;
  });
}

function decodeVectorBits(value: unknown, dimension: number): ReadonlyArray<number> {
  if (!Array.isArray(value) || value.length !== dimension) return fail();
  const float = new Float32Array(1);
  const bits = new Uint32Array(float.buffer);
  return value.map((componentBits) => {
    bits[0] = boundedInteger(componentBits, 0, 4_294_967_295);
    const component = float[0]!;
    if (!Number.isFinite(component)) return fail();
    return component === 0 ? 0 : component;
  });
}

function decodeVector(value: unknown, dimension: number | undefined): ReadonlyArray<number> {
  if (!Array.isArray(value) || value.length < 1 || value.length > 4_096
      || (dimension !== undefined && value.length !== dimension)) return fail();
  return value.map((component) => {
    if (typeof component !== "number" || !Number.isFinite(component)) return fail();
    const rounded = Math.fround(component);
    if (!Number.isFinite(rounded) || rounded !== component) return fail();
    return component === 0 ? 0 : component;
  });
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
function signedTwosComplement(value: bigint): Uint8Array {
  for (let width = 1; width <= 16; width += 1) {
    const bits = BigInt(width * 8);
    const minimum = -(1n << (bits - 1n));
    const maximum = (1n << (bits - 1n)) - 1n;
    if (value < minimum || value > maximum) continue;
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

export {
  DRIVER_ERROR_REGISTRY_HASH,
  DRIVER_PROTOCOL_VERSION,
  DRIVER_VALUE_REGISTRY_HASH,
  DriverApplicationError,
  DriverApplicationTransport,
} from "./driver.js";
export type {
  DriverApplicationIdentity,
  DriverApplicationTransportOptions,
  DriverBatchItem,
  DriverBatchResult,
  DriverBatchSuccess,
  DriverDecimal,
  DriverErrorDetails,
  DriverInvokeOptions,
  DriverMoney,
  DriverOperation,
  DriverPackedColumn,
  DriverPackedQueryResult,
  DriverResult,
  DriverTimestamp,
  DriverValue,
} from "./driver.js";
