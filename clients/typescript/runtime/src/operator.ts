import { createConnection, type Socket } from "node:net";

const MAX_OPERATOR_FRAME_BYTES = 4 * 1_024 * 1_024 + 256 * 1_024;
const MAX_PAGE_ITEMS = 500;
const HASH = /^[0-9a-f]{64}$/;
const UUID7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

/** Exact private reimport-driver protocol generation. */
export const OPERATOR_DRIVER_PROTOCOL_VERSION = 1 as const;

export interface OperatorIdentity {
  readonly database: string;
  readonly campaignId: string;
  readonly portabilityManifestHash: string;
}

export interface ReimportOperation {
  readonly campaignId: string;
  readonly contractLineage: string;
  readonly scope: string;
  readonly portabilityManifestHash: string;
  readonly exportManifestHash: string;
  readonly exportReceiptHash: string;
  readonly sourceDatabaseId: string;
  readonly targetDatabaseId: string;
  readonly sourceRows: bigint;
  readonly sourcePages: bigint;
  readonly nextPage: bigint;
  readonly rowsApplied: bigint;
  readonly phase: string;
  readonly failure?: string;
  readonly canonicalReimportReceiptJson?: string;
  readonly reimportReceiptHash?: string;
}

export interface ReimportPage {
  readonly exportOperationId: string;
  readonly pageNumber: bigint;
  readonly canonicalJsonLines: ReadonlyArray<string>;
  readonly nextCursorBase64?: string;
  readonly classComplete: boolean;
  readonly operationComplete: boolean;
  readonly pageHashHex: string;
  readonly maximumAttempts?: number;
}

export interface OperatorErrorDetails {
  readonly code: string;
  readonly category: string;
  readonly message: string;
  readonly retryability: string;
  readonly recoveryAction: string;
  readonly outcomeUncertain: boolean;
}

/** Typed public-safe failure returned by the Rust operator host. */
export class OperatorError extends Error {
  public override readonly name = "OperatorError";
  public constructor(public readonly details: OperatorErrorDetails) {
    super(`${details.code}: ${details.message}`);
  }
}

/**
 * One serial connection to a Rust-configured reimport campaign.
 *
 * This surface has no application invocation, remote endpoint, TLS, bearer
 * credential, manifest selection, or raw import method.
 */
export class OperatorTransport {
  readonly #socket: Socket;
  readonly #identity: OperatorIdentity;
  #input = Buffer.alloc(0);
  #pending: { readonly id: string; readonly resolve: (value: WireObject) => void; readonly reject: (error: Error) => void } | undefined;
  #nextRequest = 1;
  #closed = false;

  private constructor(socket: Socket, identity: OperatorIdentity) {
    this.#socket = socket;
    this.#identity = identity;
    socket.on("data", (chunk: Buffer) => this.#receive(chunk));
    socket.on("error", () => this.#fail(new Error("RiffDB operator session failed")));
    socket.on("close", () => this.#fail(new Error("RiffDB operator session closed")));
  }

  public static async connect(socketPath: string, identity: OperatorIdentity): Promise<OperatorTransport> {
    validateIdentity(socketPath, identity);
    const socket = createConnection({ path: socketPath });
    await new Promise<void>((resolve, reject) => {
      const onError = (): void => reject(new Error("RiffDB operator session failed"));
      socket.once("connect", () => { socket.off("error", onError); resolve(); });
      socket.once("error", onError);
    });
    const transport = new OperatorTransport(socket, identity);
    try {
      const requestId = transport.#requestId("handshake");
      const response = await transport.#request({
        type: "handshake",
        request_id: requestId,
        protocol_version: OPERATOR_DRIVER_PROTOCOL_VERSION,
        database: identity.database,
        campaign_id: identity.campaignId,
        portability_manifest_hash: identity.portabilityManifestHash,
      }, requestId);
      if (!hasExactKeys(response, ["type", "request_id", "protocol_version", "driver_identity", "database", "campaign_id", "portability_manifest_hash"])
          || response.type !== "handshake"
          || response.protocol_version !== OPERATOR_DRIVER_PROTOCOL_VERSION
          || typeof response.driver_identity !== "string" || response.driver_identity.length === 0
          || response.database !== identity.database || response.campaign_id !== identity.campaignId
          || response.portability_manifest_hash !== identity.portabilityManifestHash) {
        throw new Error("RiffDB operator identity mismatch");
      }
      return transport;
    } catch (error) {
      socket.destroy();
      throw error;
    }
  }

  public async start(canonicalExportManifestJson: string, canonicalExportReceiptJson: string, maximumAttempts = 3): Promise<ReimportOperation> {
    if (!canonicalDocument(canonicalExportManifestJson, 256 * 1_024)
        || !canonicalDocument(canonicalExportReceiptJson, 256 * 1_024)) {
      throw new Error("invalid RiffDB operator start request");
    }
    const requestId = this.#requestId("start");
    const response = await this.#request({
      type: "start", request_id: requestId,
      canonical_export_manifest_json: canonicalExportManifestJson,
      canonical_export_receipt_json: canonicalExportReceiptJson,
      maximum_attempts: checkedAttempts(maximumAttempts),
    }, requestId);
    return requiredOperation(response);
  }

  public async applyPage(page: ReimportPage): Promise<ReimportOperation> {
    validatePage(page);
    const requestId = this.#requestId("page");
    const response = await this.#request({
      type: "apply_page", request_id: requestId,
      export_operation_id: page.exportOperationId,
      page_number: page.pageNumber,
      canonical_json_lines: page.canonicalJsonLines,
      next_cursor_base64: page.nextCursorBase64 ?? null,
      class_complete: page.classComplete,
      operation_complete: page.operationComplete,
      page_hash_hex: page.pageHashHex,
      maximum_attempts: checkedAttempts(page.maximumAttempts ?? 3),
    }, requestId);
    return requiredOperation(response);
  }

  public async status(): Promise<ReimportOperation | undefined> {
    const requestId = this.#requestId("status");
    return optionalOperation(await this.#request({ type: "status", request_id: requestId }, requestId));
  }

  public async cancel(maximumAttempts = 3): Promise<ReimportOperation | undefined> {
    const requestId = this.#requestId("cancel");
    return optionalOperation(await this.#request({
      type: "cancel", request_id: requestId, maximum_attempts: checkedAttempts(maximumAttempts),
    }, requestId));
  }

  public async shutdown(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#fail(new Error("RiffDB operator session closed"));
    await new Promise<void>((resolve) => {
      if (this.#socket.destroyed) { resolve(); return; }
      this.#socket.end(resolve);
    });
  }

  async #request(request: WireObject, requestId: string): Promise<WireObject> {
    if (this.#closed || this.#socket.destroyed) throw new Error("RiffDB operator session closed");
    if (this.#pending !== undefined) throw new Error("RiffDB operator session is busy");
    const frame = encodeFrame(request);
    const response = new Promise<WireObject>((resolve, reject) => {
      this.#pending = { id: requestId, resolve, reject };
      this.#socket.write(frame, (error) => {
        if (error === null || error === undefined) return;
        this.#pending = undefined;
        reject(new Error("RiffDB operator session failed"));
      });
    });
    const value = await response;
    if (value.type === "error") throw decodeError(value);
    return value;
  }

  #receive(chunk: Buffer): void {
    if (this.#closed) return;
    this.#input = Buffer.concat([this.#input, chunk]);
    while (this.#input.length >= 4) {
      const length = this.#input.readUInt32BE(0);
      if (length < 2 || length > MAX_OPERATOR_FRAME_BYTES) {
        this.#fail(new Error("RiffDB operator returned an invalid frame"));
        this.#socket.destroy();
        return;
      }
      if (this.#input.length < length + 4) return;
      const body = this.#input.subarray(4, length + 4);
      this.#input = this.#input.subarray(length + 4);
      let response: WireObject;
      try {
        const decoded: unknown = JSON.parse(body.toString("utf8"));
        response = exactObject(decoded);
      } catch {
        this.#fail(new Error("RiffDB operator returned an invalid response"));
        this.#socket.destroy();
        return;
      }
      const pending = this.#pending;
      if (pending === undefined || response.request_id !== pending.id) {
        this.#fail(new Error("RiffDB operator returned an invalid request identity"));
        this.#socket.destroy();
        return;
      }
      this.#pending = undefined;
      pending.resolve(response);
    }
  }

  #fail(error: Error): void {
    const pending = this.#pending;
    this.#pending = undefined;
    pending?.reject(error);
  }

  #requestId(kind: string): string {
    const value = `ts.operator.${kind}.${this.#nextRequest}`;
    this.#nextRequest = this.#nextRequest === Number.MAX_SAFE_INTEGER ? 1 : this.#nextRequest + 1;
    return value;
  }
}

type WireObject = Record<string, unknown>;

function encodeFrame(value: WireObject): Buffer {
  const body = Buffer.from(encodeCanonical(value), "utf8");
  if (body.length < 2 || body.length > MAX_OPERATOR_FRAME_BYTES) throw new Error("RiffDB operator request exceeds the frame bound");
  const output = Buffer.allocUnsafe(body.length + 4);
  output.writeUInt32BE(body.length, 0);
  body.copy(output, 4);
  return output;
}

function encodeCanonical(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error("invalid RiffDB operator integer");
    return value.toString();
  }
  if (typeof value === "bigint") {
    if (value < 0n || value > 18_446_744_073_709_551_615n) throw new Error("invalid RiffDB operator integer");
    return value.toString();
  }
  if (Array.isArray(value)) return `[${value.map(encodeCanonical).join(",")}]`;
  if (typeof value === "object" && value !== null) {
    return `{${Object.entries(value).map(([name, item]) => `${JSON.stringify(name)}:${encodeCanonical(item)}`).join(",")}}`;
  }
  throw new Error("invalid RiffDB operator request");
}

function validateIdentity(socketPath: string, value: OperatorIdentity): void {
  if (!socketPath.startsWith("/") || socketPath.length > 4_096 || !short(value.database)
      || !UUID7.test(value.campaignId) || !HASH.test(value.portabilityManifestHash)) {
    throw new Error("invalid RiffDB operator configuration");
  }
}

function validatePage(value: ReimportPage): void {
  const bytes = value.canonicalJsonLines.reduce((total, item) => total + Buffer.byteLength(item) + 1, 0);
  if (!UUID7.test(value.exportOperationId) || value.pageNumber < 1n
      || value.canonicalJsonLines.length < 1 || value.canonicalJsonLines.length > MAX_PAGE_ITEMS
      || bytes > 4 * 1_024 * 1_024 || value.canonicalJsonLines.some((line) => !canonicalDocument(line, 64 * 1_024))
      || !HASH.test(value.pageHashHex) || value.operationComplete !== (value.nextCursorBase64 === undefined)
      || (value.operationComplete && !value.classComplete)
      || (value.nextCursorBase64 !== undefined && (value.nextCursorBase64.length < 1 || value.nextCursorBase64.length > 1_024))) {
    throw new Error("invalid RiffDB operator page");
  }
}

function requiredOperation(response: WireObject): ReimportOperation {
  const operation = optionalOperation(response);
  if (operation === undefined) throw new Error("RiffDB operator campaign was not found");
  return operation;
}

function optionalOperation(response: WireObject): ReimportOperation | undefined {
  if (response.type === "not_found" && hasExactKeys(response, ["type", "request_id"])) return undefined;
  if (response.type !== "operation" || !hasExactKeys(response, ["type", "request_id", "operation"])) {
    throw new Error("RiffDB operator returned an invalid response");
  }
  const value = exactObject(response.operation);
  if (!hasExactKeys(value, [
    "campaign_id", "contract_lineage", "scope", "portability_manifest_hash",
    "export_manifest_hash", "export_receipt_hash", "source_database_id", "target_database_id",
    "source_rows", "source_pages", "next_page", "rows_applied", "phase", "failure",
    "canonical_reimport_receipt_json", "reimport_receipt_hash",
  ])) throw new Error("RiffDB operator returned invalid progress");
  const failure = optionalShort(value.failure);
  const receipt = optionalCanonicalReceipt(value.canonical_reimport_receipt_json);
  const receiptHash = optionalHash(value.reimport_receipt_hash);
  return {
    campaignId: uuid7(value.campaign_id), contractLineage: requiredShort(value.contract_lineage),
    scope: requiredShort(value.scope), portabilityManifestHash: requiredHash(value.portability_manifest_hash),
    exportManifestHash: requiredHash(value.export_manifest_hash), exportReceiptHash: requiredHash(value.export_receipt_hash),
    sourceDatabaseId: uuid7(value.source_database_id), targetDatabaseId: uuid7(value.target_database_id),
    sourceRows: unsigned(value.source_rows), sourcePages: unsigned(value.source_pages), nextPage: unsigned(value.next_page),
    rowsApplied: unsigned(value.rows_applied), phase: requiredShort(value.phase),
    ...(failure === undefined ? {} : { failure }),
    ...(receipt === undefined ? {} : { canonicalReimportReceiptJson: receipt }),
    ...(receiptHash === undefined ? {} : { reimportReceiptHash: receiptHash }),
  };
}

function decodeError(value: WireObject): OperatorError {
  if (!hasExactKeys(value, ["type", "request_id", "code", "category", "message", "retryability", "recovery_action", "outcome_uncertain"])
      || value.type !== "error" || typeof value.outcome_uncertain !== "boolean") {
    throw new Error("RiffDB operator returned an invalid error");
  }
  return new OperatorError({
    code: requiredShort(value.code), category: requiredShort(value.category), message: requiredShort(value.message),
    retryability: requiredShort(value.retryability), recoveryAction: requiredShort(value.recovery_action),
    outcomeUncertain: value.outcome_uncertain,
  });
}

function exactObject(value: unknown): WireObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error("invalid RiffDB operator object");
  return value as WireObject;
}
function hasExactKeys(value: WireObject, keys: ReadonlyArray<string>): boolean {
  const actual = Object.keys(value);
  return actual.length === keys.length && keys.every((key) => Object.hasOwn(value, key));
}
function short(value: string): boolean { return value.length > 0 && value.length <= 4_096 && !/[\n\r\0]/u.test(value); }
function requiredShort(value: unknown): string { if (typeof value !== "string" || !short(value)) throw new Error("invalid RiffDB operator text"); return value; }
function optionalShort(value: unknown): string | undefined { return value === null || value === undefined ? undefined : requiredShort(value); }
function requiredHash(value: unknown): string { if (typeof value !== "string" || !HASH.test(value)) throw new Error("invalid RiffDB operator hash"); return value; }
function optionalHash(value: unknown): string | undefined { return value === null || value === undefined ? undefined : requiredHash(value); }
function uuid7(value: unknown): string { if (typeof value !== "string" || !UUID7.test(value)) throw new Error("invalid RiffDB operator identity"); return value; }
function unsigned(value: unknown): bigint { if (typeof value !== "string" || !/^(?:0|[1-9][0-9]*)$/u.test(value)) throw new Error("invalid RiffDB operator count"); return BigInt(value); }
function checkedAttempts(value: number): number { if (!Number.isInteger(value) || value < 1 || value > 10) throw new Error("invalid RiffDB operator attempt bound"); return value; }
function canonicalDocument(value: string, maximum: number): boolean { return Buffer.byteLength(value) <= maximum && value.startsWith("{") && value.endsWith("}") && !/[\n\r]/u.test(value); }
function optionalCanonicalReceipt(value: unknown): string | undefined {
  if (value === null || value === undefined) return undefined;
  if (typeof value !== "string" || !value.endsWith("\n") || !canonicalDocument(value.slice(0, -1), 256 * 1_024 - 1)) throw new Error("invalid RiffDB operator receipt");
  return value;
}
