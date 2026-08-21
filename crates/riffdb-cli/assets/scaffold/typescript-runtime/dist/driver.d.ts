/** Exact alpha driver protocol generation. */
export declare const DRIVER_PROTOCOL_VERSION: 2;
/** Exact tagged value registry compiled into `riffdb-driverd`. */
export declare const DRIVER_VALUE_REGISTRY_HASH: "8e1681ddf5e6a82e7fa646f9737128ad7e36f54f8b5846ac6e33e732125407e5";
/** Exact structured-error registry compiled into `riffdb-driverd`. */
export declare const DRIVER_ERROR_REGISTRY_HASH: "b94d685ecbc18f2369a2bfa1a53139d06100699c4ee41b31c86d6a7e17039850";
export type DriverValue = {
    readonly type: "null";
} | {
    readonly type: "bool";
    readonly value: boolean;
} | {
    readonly type: "i64" | "u64" | "string" | "uuid" | "enum" | "bytes" | "date";
    readonly value: string;
} | {
    readonly type: "timestamp";
    readonly value: DriverTimestamp;
} | {
    readonly type: "decimal";
    readonly value: DriverDecimal;
} | {
    readonly type: "money";
    readonly value: DriverMoney;
} | {
    readonly type: "vector";
    readonly value: DriverVector;
} | {
    readonly type: "list";
    readonly value: ReadonlyArray<DriverValue>;
} | {
    readonly type: "record";
    readonly value: Readonly<Record<string, DriverValue>>;
};
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
export interface DriverVector {
    readonly component_bits: ReadonlyArray<number>;
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
export declare function exactDecimal(value: string, precision: number, scale: number): ExactDecimalValue;
export declare function exactMoney<const Currency extends string>(currency: Currency, value: string): ExactMoneyValue<Currency>;
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
export declare class DriverApplicationError extends Error {
    readonly details: DriverErrorDetails;
    readonly name = "DriverApplicationError";
    constructor(details: DriverErrorDetails);
}
/**
 * One retained, bounded local session to `riffdb-driverd`.
 *
 * This class has no remote endpoint, credential, TLS, gRPC, or retry-status
 * surface. Those semantics remain owned by the Rust host.
 */
export declare class DriverApplicationTransport {
    #private;
    private constructor();
    static connect(options: DriverApplicationTransportOptions): Promise<DriverApplicationTransport>;
    invoke(operation: DriverOperation, input: Readonly<Record<string, DriverValue>>, options?: DriverInvokeOptions): Promise<DriverResult>;
    batch(operation: DriverOperation, items: ReadonlyArray<Readonly<Record<string, DriverValue>>>, concurrency: number, checkpoint: number, options?: DriverInvokeOptions): Promise<DriverBatchResult>;
    /** Stops new work, sends a clean local EOF, and rejects unfinished waits. */
    shutdown(): Promise<void>;
}
