import { createConnection, type Socket } from "node:net";

const MAX_FRAME_BYTES = 1_048_576;
const MAX_PENDING_REQUESTS = 256;
const MAX_COLLECTION_ITEMS = 4_096;
const MAX_VALUE_DEPTH = 32;
const HASH = /^[0-9a-f]{64}$/;
const SYMBOL = /^[A-Za-z0-9_.-]{1,256}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const REQUEST_ID = /^[A-Za-z0-9_.-]{1,128}$/;
const MAX_U64 = 18_446_744_073_709_551_615n;
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });
let nextSession = 0;

/** Exact alpha driver protocol generation. */
export const DRIVER_PROTOCOL_VERSION = 2 as const;
/** Exact tagged value registry compiled into `riffdb-driverd`. */
export const DRIVER_VALUE_REGISTRY_HASH = "8660841ce2055895ba4e1999836b0be11792651c7dcf166f21cf1b43c0dd86af" as const;
/** Exact structured-error registry compiled into `riffdb-driverd`. */
export const DRIVER_ERROR_REGISTRY_HASH = "b94d685ecbc18f2369a2bfa1a53139d06100699c4ee41b31c86d6a7e17039850" as const;

export type DriverValue =
  | { readonly type: "null" }
  | { readonly type: "bool"; readonly value: boolean }
  | { readonly type: "i64" | "u64" | "string" | "uuid" | "enum" | "bytes" | "date"; readonly value: string }
  | { readonly type: "timestamp"; readonly value: DriverTimestamp }
  | { readonly type: "decimal"; readonly value: DriverDecimal }
  | { readonly type: "money"; readonly value: DriverMoney }
  | { readonly type: "list"; readonly value: ReadonlyArray<DriverValue> }
  | { readonly type: "record"; readonly value: Readonly<Record<string, DriverValue>> };

export interface DriverTimestamp {
  readonly seconds: string;
  readonly nanos: number;
}

export interface DriverDecimal {
  readonly coefficient: string;
  readonly scale: number;
  readonly precision?: number | null;
}

export interface DriverMoney {
  readonly currency: string;
  readonly amount: DriverDecimal;
}

export interface ExactDecimalValue {
  readonly coefficientTwosComplement: Uint8Array;
  readonly scale: number;
  readonly precision: number;
}

export interface ExactMoneyValue<Currency extends string = string> {
  readonly currency: Currency;
  readonly amount: ExactDecimalValue;
}

export function exactDecimal(value: string, precision: number, scale: number): ExactDecimalValue {
  if (!Number.isInteger(precision) || precision < 1 || precision > 38
      || !Number.isInteger(scale) || scale < 0 || scale > precision) {
    throw new Error("invalid exact decimal type");
  }
  const match = /^(-?)([0-9]+)(?:\.([0-9]+))?$/.exec(value);
  if (match === null || value.length > 128) throw new Error("invalid exact decimal text");
  const fractional = match[3] ?? "";
  if (fractional.length > scale) throw new Error("exact decimal exceeds scale");
  let coefficient = BigInt(`${match[2]}${fractional.padEnd(scale, "0")}`);
  if (match[1] === "-" && coefficient !== 0n) coefficient = -coefficient;
  const limit = 10n ** BigInt(precision);
  if (coefficient <= -limit || coefficient >= limit) throw new Error("exact decimal exceeds precision");
  return { coefficientTwosComplement: signedTwosComplement(coefficient), scale, precision };
}

export function exactMoney<const Currency extends string>(
  currency: Currency,
  value: string,
): ExactMoneyValue<Currency> {
  if (!/^[A-Z]{3}$/.test(currency)) throw new Error("invalid exact money currency");
  return { currency, amount: exactDecimal(value, 38, 2) };
}

export interface DriverApplicationIdentity {
  readonly applicationManifestHash: string;
  readonly operationCatalogHash: string;
  readonly contractLineage: string;
  readonly contractVersion: bigint;
  readonly contractBundleHash: string;
  readonly database: string;
  readonly role: string;
  readonly roleDefinitionHash: string;
  readonly remoteIdentityHash: string;
}

export interface DriverApplicationTransportOptions {
  readonly socketPath: string;
  readonly identity: DriverApplicationIdentity;
}

export interface DriverOperation {
  readonly name: string;
  readonly inputSchemaHash: string;
}

export interface DriverInvokeOptions {
  readonly deadlineMillis?: number;
  readonly maximumAttempts?: number;
  readonly readAfterCommit?: bigint;
  readonly cursor?: string;
  readonly signal?: AbortSignal;
  readonly acceptCompactResult?: boolean;
}

export interface DriverCompactQueryResult {
  readonly outcome: string;
  readonly resultName: string;
  readonly entity: string;
  readonly fields: ReadonlyArray<string>;
  readonly rows: ReadonlyArray<ReadonlyArray<DriverValue>>;
}

export interface DriverResult {
  readonly value?: DriverValue;
  readonly compact?: DriverCompactQueryResult;
  readonly applicationHead?: bigint;
  readonly cursor?: string;
  readonly replayed: boolean;
}

export interface DriverBatchSuccess {
  readonly value: DriverValue;
  readonly commitSequence?: bigint;
  readonly outcomeUri?: string;
  readonly replayed: boolean;
}

export interface DriverBatchItem {
  readonly index: number;
  readonly result?: DriverBatchSuccess;
  readonly error?: DriverApplicationError;
}

export interface DriverBatchResult {
  readonly items: ReadonlyArray<DriverBatchItem>;
  readonly checkpoint: number;
  readonly total: number;
}

export interface DriverErrorDetails {
  readonly code: string;
  readonly category: string;
  readonly operation?: string;
  readonly symbolPath: ReadonlyArray<string>;
  readonly contractLineage?: string;
  readonly contractVersion?: bigint;
  readonly traceId?: string;
  readonly incidentId?: string;
  readonly message: string;
  readonly retryability: string;
  readonly recoveryAction: string;
  readonly outcomeUncertain: boolean;
}

/** Checked public error returned by the Rust driver host. */
export class DriverApplicationError extends Error {
  public override readonly name = "DriverApplicationError";

  public constructor(public readonly details: DriverErrorDetails) {
    super(`${details.code}: ${details.message}`);
  }
}

type DriverResponse = Record<string, unknown>;
interface PendingRequest {
  readonly resolve: (response: DriverResponse) => void;
  readonly reject: (error: Error) => void;
}

/**
 * One retained, bounded local session to `riffdb-driverd`.
 *
 * This class has no remote endpoint, credential, TLS, gRPC, or retry-status
 * surface. Those semantics remain owned by the Rust host.
 */
export class DriverApplicationTransport {
  readonly #socket: Socket;
  readonly #identity: DriverApplicationIdentity;
  readonly #requestPrefix: string;
  readonly #pending = new Map<string, PendingRequest>();
  #input = Buffer.alloc(0);
  #nextRequest = 1;
  #closed = false;

  private constructor(socket: Socket, identity: DriverApplicationIdentity) {
    this.#socket = socket;
    this.#identity = identity;
    nextSession = nextSession === Number.MAX_SAFE_INTEGER ? 1 : nextSession + 1;
    this.#requestPrefix = `ts.${process.pid}.${nextSession}`;
    socket.on("data", (chunk: Buffer) => this.#receive(chunk));
    socket.on("error", () => this.#fail(new Error("RiffDB driver session failed")));
    socket.on("close", () => this.#fail(new Error("RiffDB driver session closed")));
  }

  public static async connect(options: DriverApplicationTransportOptions): Promise<DriverApplicationTransport> {
    validateTransportOptions(options);
    const socket = createConnection({ path: options.socketPath });
    await new Promise<void>((resolve, reject) => {
      const onError = (): void => reject(new Error("RiffDB driver session failed"));
      socket.once("connect", () => { socket.off("error", onError); resolve(); });
      socket.once("error", onError);
    });
    const transport = new DriverApplicationTransport(socket, options.identity);
    try {
      await transport.#handshake();
      return transport;
    } catch (error) {
      socket.destroy();
      throw error;
    }
  }

  public async invoke(
    operation: DriverOperation,
    input: Readonly<Record<string, DriverValue>>,
    options: DriverInvokeOptions = {},
  ): Promise<DriverResult> {
    validateOperation(operation);
    validateDriverRecord(input);
    const requestId = this.#requestId("invoke");
    const response = await this.#request({
      type: "invoke",
      request_id: requestId,
      operation: operation.name,
      input_schema_hash: operation.inputSchemaHash,
      input,
      options: lowerOptions(options),
    }, requestId, options.signal);
    return decodeResult(response);
  }

  public async batch(
    operation: DriverOperation,
    items: ReadonlyArray<Readonly<Record<string, DriverValue>>>,
    concurrency: number,
    checkpoint: number,
    options: DriverInvokeOptions = {},
  ): Promise<DriverBatchResult> {
    validateOperation(operation);
    if (items.length < 1 || items.length > MAX_COLLECTION_ITEMS
        || !Number.isInteger(concurrency) || concurrency < 1 || concurrency > 384
        || !Number.isInteger(checkpoint) || checkpoint < 0 || checkpoint > items.length) {
      throw new Error("invalid RiffDB driver batch bounds");
    }
    for (const input of items) validateDriverRecord(input);
    const requestId = this.#requestId("batch");
    const response = await this.#request({
      type: "batch",
      request_id: requestId,
      operation: operation.name,
      input_schema_hash: operation.inputSchemaHash,
      items,
      concurrency,
      checkpoint,
      options: lowerOptions(options),
    }, requestId, options.signal);
    return decodeBatchResult(response);
  }

  /** Stops new work, sends a clean local EOF, and rejects unfinished waits. */
  public async shutdown(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#fail(new Error("RiffDB driver session closed"));
    await new Promise<void>((resolve) => {
      if (this.#socket.destroyed) { resolve(); return; }
      this.#socket.end(resolve);
    });
  }

  async #handshake(): Promise<void> {
    const requestId = this.#requestId("handshake");
    const identity = this.#identity;
    const response = await this.#request({
      type: "handshake",
      request_id: requestId,
      protocol_version: DRIVER_PROTOCOL_VERSION,
      application_manifest_hash: identity.applicationManifestHash,
      operation_catalog_hash: identity.operationCatalogHash,
      contract_lineage: identity.contractLineage,
      contract_version: identity.contractVersion,
      contract_bundle_hash: identity.contractBundleHash,
      value_registry_hash: DRIVER_VALUE_REGISTRY_HASH,
      error_registry_hash: DRIVER_ERROR_REGISTRY_HASH,
      database: identity.database,
      role: identity.role,
      role_definition_hash: identity.roleDefinitionHash,
      remote_identity_hash: identity.remoteIdentityHash,
    }, requestId);
    if (response.type !== "handshake"
        || response.protocol_version !== DRIVER_PROTOCOL_VERSION
        || response.application_manifest_hash !== identity.applicationManifestHash
        || response.operation_catalog_hash !== identity.operationCatalogHash
        || response.contract_lineage !== identity.contractLineage
        || requiredPositiveBigInt(response.contract_version) !== identity.contractVersion
        || response.contract_bundle_hash !== identity.contractBundleHash
        || response.database !== identity.database
        || response.role !== identity.role
        || response.role_definition_hash !== identity.roleDefinitionHash
        || response.remote_identity_hash !== identity.remoteIdentityHash) {
      throw new Error("RiffDB driver identity mismatch");
    }
  }

  async #request(
    request: Record<string, unknown>,
    requestId: string,
    signal?: AbortSignal,
  ): Promise<DriverResponse> {
    if (this.#closed || this.#socket.destroyed) throw new Error("RiffDB driver session closed");
    if (this.#pending.size >= MAX_PENDING_REQUESTS) throw new Error("RiffDB driver session is over capacity");
    if (signal?.aborted === true) throw new Error("RiffDB driver request was cancelled before submission");
    const frame = encodeFrame(request);
    let abort: (() => void) | undefined;
    const response = new Promise<DriverResponse>((resolve, reject) => {
      this.#pending.set(requestId, { resolve, reject });
      if (signal !== undefined) {
        abort = (): void => {
          if (!this.#pending.has(requestId) || this.#closed) return;
          const cancelId = this.#requestId("cancel");
          this.#socket.write(encodeFrame({
            type: "cancel",
            request_id: cancelId,
            target_request_id: requestId,
          }));
        };
        signal.addEventListener("abort", abort, { once: true });
      }
      this.#socket.write(frame, (error) => {
        if (error === null || error === undefined) return;
        this.#pending.delete(requestId);
        reject(new Error("RiffDB driver session failed"));
      });
    });
    try {
      const value = await response;
      if (value.type === "error") throw decodeError(value);
      return value;
    } finally {
      if (signal !== undefined && abort !== undefined) signal.removeEventListener("abort", abort);
    }
  }

  #receive(chunk: Buffer): void {
    if (this.#closed) return;
    this.#input = Buffer.concat([this.#input, chunk]);
    while (this.#input.length >= 4) {
      const length = this.#input.readUInt32BE(0);
      if (length < 2 || length > MAX_FRAME_BYTES) {
        this.#fail(new Error("RiffDB driver returned an invalid frame"));
        this.#socket.destroy();
        return;
      }
      if (this.#input.length < length + 4) return;
      const body = this.#input.subarray(4, length + 4);
      this.#input = this.#input.subarray(length + 4);
      let response: DriverResponse;
      try {
        response = exactObject(decodeJson(UTF8_DECODER.decode(body)));
      } catch {
        this.#fail(new Error("RiffDB driver returned an invalid message"));
        this.#socket.destroy();
        return;
      }
      const requestId = response.request_id;
      if (typeof requestId !== "string" || !REQUEST_ID.test(requestId)) {
        this.#fail(new Error("RiffDB driver returned an invalid request identity"));
        this.#socket.destroy();
        return;
      }
      const pending = this.#pending.get(requestId);
      if (pending === undefined) continue;
      this.#pending.delete(requestId);
      pending.resolve(response);
    }
  }

  #fail(error: Error): void {
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
  }

  #requestId(kind: string): string {
    const value = `${this.#requestPrefix}.${kind}.${this.#nextRequest}`;
    this.#nextRequest = this.#nextRequest === Number.MAX_SAFE_INTEGER ? 1 : this.#nextRequest + 1;
    return value;
  }
}

function lowerOptions(options: DriverInvokeOptions): Record<string, unknown> {
  const deadline = options.deadlineMillis ?? 30_000;
  const attempts = options.maximumAttempts ?? 3;
  if (!Number.isInteger(deadline) || deadline < 1 || deadline > 300_000
      || !Number.isInteger(attempts) || attempts < 1 || attempts > 10
      || (options.readAfterCommit !== undefined && options.readAfterCommit < 1n)
      || (options.cursor !== undefined && options.cursor.length > 16_384)) {
    throw new Error("invalid RiffDB driver invocation options");
  }
  return {
    deadline_millis: deadline,
    maximum_attempts: attempts,
    read_after_commit: options.readAfterCommit ?? null,
    cursor: options.cursor ?? null,
    accept_compact_result: options.acceptCompactResult ?? false,
  };
}

function encodeFrame(value: Record<string, unknown>): Buffer {
  const body = Buffer.from(encodeJson(value), "utf8");
  if (body.length < 2 || body.length > MAX_FRAME_BYTES) throw new Error("RiffDB driver request exceeds the frame bound");
  const frame = Buffer.allocUnsafe(body.length + 4);
  frame.writeUInt32BE(body.length, 0);
  body.copy(frame, 4);
  return frame;
}

/**
 * Encodes the closed protocol without coercing u64 frontiers through the
 * JavaScript `number` domain. Only integral JSON numbers are admitted.
 */
function encodeJson(value: unknown, depth = 0): string {
  if (depth > MAX_VALUE_DEPTH + 4) throw new Error("RiffDB driver message exceeds the depth bound");
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "bigint") {
    if (value < 0n || value > MAX_U64) throw new Error("invalid RiffDB driver integer");
    return value.toString();
  }
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) throw new Error("invalid RiffDB driver integer");
    return value.toString();
  }
  if (Array.isArray(value)) {
    if (value.length > MAX_COLLECTION_ITEMS) throw new Error("RiffDB driver collection exceeds its bound");
    return `[${value.map((item) => encodeJson(item, depth + 1)).join(",")}]`;
  }
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value).filter(([, item]) => item !== undefined);
    if (entries.length > MAX_COLLECTION_ITEMS) throw new Error("RiffDB driver record exceeds its bound");
    entries.sort(([left], [right]) => left.localeCompare(right));
    return `{${entries.map(([name, item]) => `${JSON.stringify(name)}:${encodeJson(item, depth + 1)}`).join(",")}}`;
  }
  throw new Error("invalid RiffDB driver message");
}

/** Parses integral JSON while preserving integers outside Number's exact range. */
function decodeJson(value: string): unknown {
  const parser = new ExactJsonParser(value);
  return parser.parse();
}

class ExactJsonParser {
  #position = 0;

  public constructor(private readonly source: string) {}

  public parse(): unknown {
    const value = this.#value(0);
    this.#space();
    if (this.#position !== this.source.length) throw new Error("invalid RiffDB driver JSON");
    return value;
  }

  #value(depth: number): unknown {
    if (depth > MAX_VALUE_DEPTH + 4) throw new Error("RiffDB driver message exceeds the depth bound");
    this.#space();
    const token = this.source[this.#position];
    if (token === '"') return this.#string();
    if (token === "{") return this.#object(depth + 1);
    if (token === "[") return this.#array(depth + 1);
    if (token === "t") return this.#literal("true", true);
    if (token === "f") return this.#literal("false", false);
    if (token === "n") return this.#literal("null", null);
    if (token === "-" || (token !== undefined && token >= "0" && token <= "9")) return this.#integer();
    throw new Error("invalid RiffDB driver JSON");
  }

  #object(depth: number): Record<string, unknown> {
    this.#position += 1;
    const output: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
    let count = 0;
    this.#space();
    if (this.source[this.#position] === "}") { this.#position += 1; return output; }
    for (;;) {
      this.#space();
      if (this.source[this.#position] !== '"') throw new Error("invalid RiffDB driver JSON");
      const name = this.#string();
      if (Object.hasOwn(output, name)) throw new Error("duplicate RiffDB driver JSON field");
      this.#space();
      if (this.source[this.#position] !== ":") throw new Error("invalid RiffDB driver JSON");
      this.#position += 1;
      output[name] = this.#value(depth);
      count += 1;
      if (count > MAX_COLLECTION_ITEMS) throw new Error("RiffDB driver record exceeds its bound");
      this.#space();
      const separator = this.source[this.#position];
      this.#position += 1;
      if (separator === "}") return output;
      if (separator !== ",") throw new Error("invalid RiffDB driver JSON");
    }
  }

  #array(depth: number): ReadonlyArray<unknown> {
    this.#position += 1;
    const output: unknown[] = [];
    this.#space();
    if (this.source[this.#position] === "]") { this.#position += 1; return output; }
    for (;;) {
      output.push(this.#value(depth));
      if (output.length > MAX_COLLECTION_ITEMS) throw new Error("RiffDB driver collection exceeds its bound");
      this.#space();
      const separator = this.source[this.#position];
      this.#position += 1;
      if (separator === "]") return output;
      if (separator !== ",") throw new Error("invalid RiffDB driver JSON");
    }
  }

  #string(): string {
    const start = this.#position;
    this.#position += 1;
    let escaped = false;
    while (this.#position < this.source.length) {
      const character = this.source[this.#position];
      this.#position += 1;
      if (escaped) { escaped = false; continue; }
      if (character === "\\") { escaped = true; continue; }
      if (character === '"') {
        const decoded: unknown = JSON.parse(this.source.slice(start, this.#position));
        if (typeof decoded !== "string") throw new Error("invalid RiffDB driver JSON");
        return decoded;
      }
      if (character !== undefined && character.charCodeAt(0) < 0x20) throw new Error("invalid RiffDB driver JSON");
    }
    throw new Error("invalid RiffDB driver JSON");
  }

  #integer(): number | string {
    const remainder = this.source.slice(this.#position);
    const match = /^-?(?:0|[1-9][0-9]*)/.exec(remainder);
    if (match === null) throw new Error("invalid RiffDB driver JSON");
    this.#position += match[0].length;
    const next = this.source[this.#position];
    if (next === "." || next === "e" || next === "E") throw new Error("non-integral RiffDB driver JSON number");
    const exact = BigInt(match[0]);
    if (exact >= BigInt(Number.MIN_SAFE_INTEGER) && exact <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(exact);
    return match[0];
  }

  #literal<T>(text: string, value: T): T {
    if (!this.source.startsWith(text, this.#position)) throw new Error("invalid RiffDB driver JSON");
    this.#position += text.length;
    return value;
  }

  #space(): void {
    while (/\s/u.test(this.source[this.#position] ?? "")) this.#position += 1;
  }
}

function decodeResult(response: DriverResponse): DriverResult {
  if (response.type === "compact_query_result") {
    const outcome = boundedPattern(response.outcome, SYMBOL);
    const resultName = boundedPattern(response.result_name, SYMBOL);
    const entity = boundedPattern(response.entity, SYMBOL);
    if (!Array.isArray(response.fields) || response.fields.length < 1
        || response.fields.length > MAX_COLLECTION_ITEMS
        || !Array.isArray(response.rows) || response.rows.length > MAX_COLLECTION_ITEMS) {
      throw new Error("RiffDB driver returned an invalid compact result");
    }
    const fields = response.fields.map((field) => boundedPattern(field, SYMBOL));
    if (new Set(fields).size !== fields.length) {
      throw new Error("RiffDB driver returned an invalid compact result");
    }
    const rows = response.rows.map((raw) => {
      if (!Array.isArray(raw) || raw.length !== fields.length) {
        throw new Error("RiffDB driver returned an invalid compact result");
      }
      return raw.map((value) => validateDriverValue(value, 0));
    });
    const head = optionalNonnegativeBigInt(response.application_head);
    if (head === undefined) throw new Error("RiffDB driver omitted the query frontier");
    const cursor = optionalBoundedString(response.cursor, 16_384);
    return {
      compact: { outcome, resultName, entity, fields, rows },
      applicationHead: head,
      replayed: false,
      ...(cursor === undefined ? {} : { cursor }),
    };
  }
  if (response.type !== "result") throw new Error("RiffDB driver returned an invalid result");
  const value = validateDriverValue(response.value, 0);
  const head = optionalNonnegativeBigInt(response.application_head);
  const cursor = optionalBoundedString(response.cursor, 16_384);
  if (typeof response.replayed !== "boolean") throw new Error("RiffDB driver returned an invalid result");
  return {
    value,
    replayed: response.replayed,
    ...(head === undefined ? {} : { applicationHead: head }),
    ...(cursor === undefined ? {} : { cursor }),
  };
}

function decodeBatchResult(response: DriverResponse): DriverBatchResult {
  if (response.type !== "batch_result" || !Array.isArray(response.items)
      || response.items.length > MAX_COLLECTION_ITEMS) {
    throw new Error("RiffDB driver returned an invalid batch result");
  }
  const checkpoint = boundedInteger(response.checkpoint, 0, MAX_COLLECTION_ITEMS);
  const total = boundedInteger(response.total, 1, MAX_COLLECTION_ITEMS);
  if (checkpoint > total) throw new Error("RiffDB driver returned an invalid batch result");
  const items = response.items.map((raw): DriverBatchItem => {
    const item = exactObject(raw);
    const index = boundedInteger(item.index, 0, total - 1);
    const outcome = exactObject(item.outcome);
    if (outcome.type === "result") {
      const value = validateDriverValue(outcome.value, 0);
      const commitSequence = optionalPositiveBigInt(outcome.commit_sequence);
      const outcomeUri = optionalBoundedString(outcome.outcome_uri, 16_384);
      if (typeof outcome.replayed !== "boolean") throw new Error("RiffDB driver returned an invalid batch item");
      return {
        index,
        result: {
          value,
          replayed: outcome.replayed,
          ...(commitSequence === undefined ? {} : { commitSequence }),
          ...(outcomeUri === undefined ? {} : { outcomeUri }),
        },
      };
    }
    if (outcome.type === "error") return { index, error: decodeError(outcome) };
    throw new Error("RiffDB driver returned an invalid batch item");
  });
  return { items, checkpoint, total };
}

function decodeError(response: DriverResponse): DriverApplicationError {
  const code = boundedPattern(response.code, /^[A-Z0-9-]{1,64}$/);
  const category = boundedPattern(response.category, /^[a-z_]{1,64}$/);
  const message = boundedString(response.message, 4_096);
  const retryability = boundedPattern(response.retryability ?? "not_retryable", /^[a-z_]{1,64}$/);
  const recoveryAction = boundedPattern(response.recovery_action, /^[a-z_]{1,64}$/);
  if (!Array.isArray(response.symbol_path) || response.symbol_path.length > 16) {
    throw new Error("RiffDB driver returned invalid error context");
  }
  const symbolPath = response.symbol_path.map((value) => boundedPattern(value, SYMBOL));
  const operation = optionalPattern(response.operation, SYMBOL);
  const contractLineage = optionalPattern(response.contract_lineage, SYMBOL);
  const contractVersion = optionalPositiveBigInt(response.contract_version);
  if ((contractLineage === undefined) !== (contractVersion === undefined)) {
    throw new Error("RiffDB driver returned invalid error context");
  }
  const traceId = optionalPattern(response.trace_id, REQUEST_ID);
  const incidentId = optionalPattern(response.incident_id, REQUEST_ID);
  if (typeof response.outcome_uncertain !== "boolean") throw new Error("RiffDB driver returned invalid error context");
  return new DriverApplicationError({
    code, category, symbolPath, message, retryability, recoveryAction,
    outcomeUncertain: response.outcome_uncertain,
    ...(operation === undefined ? {} : { operation }),
    ...(contractLineage === undefined ? {} : { contractLineage }),
    ...(contractVersion === undefined ? {} : { contractVersion }),
    ...(traceId === undefined ? {} : { traceId }),
    ...(incidentId === undefined ? {} : { incidentId }),
  });
}

function validateTransportOptions(options: DriverApplicationTransportOptions): void {
  if (!options.socketPath.startsWith("/") || options.socketPath.length > 4_096) {
    throw new Error("invalid RiffDB driver socket path");
  }
  const identity = options.identity;
  for (const value of [
    identity.applicationManifestHash, identity.operationCatalogHash,
    identity.contractBundleHash, identity.roleDefinitionHash, identity.remoteIdentityHash,
  ]) {
    if (!HASH.test(value)) throw new Error("invalid RiffDB driver identity");
  }
  if (!SYMBOL.test(identity.contractLineage) || !SYMBOL.test(identity.database)
      || !SYMBOL.test(identity.role) || identity.contractVersion < 1n
      || identity.contractVersion > 18_446_744_073_709_551_615n) {
    throw new Error("invalid RiffDB driver identity");
  }
}

function validateOperation(operation: DriverOperation): void {
  if (!SYMBOL.test(operation.name) || !HASH.test(operation.inputSchemaHash)) {
    throw new Error("invalid generated RiffDB operation");
  }
}

function validateDriverRecord(value: Readonly<Record<string, DriverValue>>, depth = 0): void {
  if (Object.keys(value).length > MAX_COLLECTION_ITEMS) throw new Error("invalid generated RiffDB input");
  for (const [name, field] of Object.entries(value)) {
    if (!SYMBOL.test(name)) throw new Error("invalid generated RiffDB input");
    validateDriverValue(field, depth);
  }
}

function validateDriverValue(value: unknown, depth: number): DriverValue {
  if (depth > MAX_VALUE_DEPTH) throw new Error("RiffDB driver value exceeds the depth bound");
  const item = exactObject(value);
  const type = item.type;
  if (type === "null" && Object.keys(item).length === 1) return item as DriverValue;
  if (type === "bool" && typeof item.value === "boolean") return item as unknown as DriverValue;
  if (type === "i64") { BigInt(boundedPattern(item.value, /^-?[0-9]{1,20}$/)); return item as unknown as DriverValue; }
  if (type === "u64") { BigInt(boundedPattern(item.value, /^[0-9]{1,20}$/)); return item as unknown as DriverValue; }
  if ((type === "string" || type === "bytes") && typeof item.value === "string" && item.value.length <= 262_144) return item as unknown as DriverValue;
  if (type === "uuid" && typeof item.value === "string" && UUID.test(item.value)) return item as unknown as DriverValue;
  if (type === "enum" && typeof item.value === "string" && SYMBOL.test(item.value)) return item as unknown as DriverValue;
  if (type === "date") { boundedPattern(item.value, /^-?[0-9]{1,11}$/); return item as unknown as DriverValue; }
  if (type === "timestamp") {
    const timestamp = exactObject(item.value);
    boundedPattern(timestamp.seconds, /^-?[0-9]{1,32}$/);
    boundedInteger(timestamp.nanos, 0, 999_999_999);
    return item as unknown as DriverValue;
  }
  if (type === "decimal") { validateDecimal(item.value); return item as unknown as DriverValue; }
  if (type === "money") {
    const money = exactObject(item.value);
    boundedPattern(money.currency, /^[A-Z]{3}$/);
    validateDecimal(money.amount);
    return item as unknown as DriverValue;
  }
  if (type === "list" && Array.isArray(item.value) && item.value.length <= MAX_COLLECTION_ITEMS) {
    item.value.forEach((entry) => validateDriverValue(entry, depth + 1));
    return item as unknown as DriverValue;
  }
  if (type === "record") {
    const record = exactObject(item.value);
    validateDriverRecord(record as Readonly<Record<string, DriverValue>>, depth + 1);
    return item as unknown as DriverValue;
  }
  throw new Error("RiffDB driver returned an invalid value");
}

function validateDecimal(value: unknown): void {
  const decimal = exactObject(value);
  boundedString(decimal.coefficient, 1_366);
  boundedInteger(decimal.scale, 0, 4_294_967_295);
  if (decimal.precision !== undefined && decimal.precision !== null) boundedInteger(decimal.precision, 1, 4_294_967_295);
}

function signedTwosComplement(value: bigint): Uint8Array {
  let bytes = 1;
  while (value < -(1n << BigInt(bytes * 8 - 1)) || value >= (1n << BigInt(bytes * 8 - 1))) bytes += 1;
  let encoded = value < 0n ? (1n << BigInt(bytes * 8)) + value : value;
  const output = new Uint8Array(bytes);
  for (let index = bytes - 1; index >= 0; index -= 1) {
    output[index] = Number(encoded & 0xffn);
    encoded >>= 8n;
  }
  return output;
}

function exactObject(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("invalid RiffDB driver message");
  return value as Record<string, unknown>;
}

function boundedString(value: unknown, maximum: number): string {
  if (typeof value !== "string" || value.length < 1 || value.length > maximum) throw new Error("invalid RiffDB driver message");
  return value;
}

function boundedPattern(value: unknown, pattern: RegExp): string {
  const text = boundedString(value, 4_096);
  if (!pattern.test(text)) throw new Error("invalid RiffDB driver message");
  return text;
}

function optionalPattern(value: unknown, pattern: RegExp): string | undefined {
  return value === null || value === undefined ? undefined : boundedPattern(value, pattern);
}

function optionalBoundedString(value: unknown, maximum: number): string | undefined {
  return value === null || value === undefined ? undefined : boundedString(value, maximum);
}

function boundedInteger(value: unknown, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || (value as number) < minimum || (value as number) > maximum) {
    throw new Error("invalid RiffDB driver message");
  }
  return value as number;
}

function optionalPositiveBigInt(value: unknown): bigint | undefined {
  if (value === null || value === undefined) return undefined;
  let parsed: bigint;
  if (typeof value === "number" && Number.isSafeInteger(value) && value > 0) parsed = BigInt(value);
  else if (typeof value === "string" && /^[1-9][0-9]{0,19}$/.test(value)) parsed = BigInt(value);
  else throw new Error("invalid RiffDB driver message");
  if (parsed > MAX_U64) throw new Error("invalid RiffDB driver message");
  return parsed;
}

function optionalNonnegativeBigInt(value: unknown): bigint | undefined {
  if (value === null || value === undefined) return undefined;
  let parsed: bigint;
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) parsed = BigInt(value);
  else if (typeof value === "string" && /^(?:0|[1-9][0-9]{0,19})$/.test(value)) parsed = BigInt(value);
  else throw new Error("invalid RiffDB driver message");
  if (parsed > MAX_U64) throw new Error("invalid RiffDB driver message");
  return parsed;
}

function requiredPositiveBigInt(value: unknown): bigint {
  const parsed = optionalPositiveBigInt(value);
  if (parsed === undefined) throw new Error("invalid RiffDB driver message");
  return parsed;
}
