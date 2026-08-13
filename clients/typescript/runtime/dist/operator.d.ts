/** Exact private reimport-driver protocol generation. */
export declare const OPERATOR_DRIVER_PROTOCOL_VERSION: 1;
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
export declare class OperatorError extends Error {
    readonly details: OperatorErrorDetails;
    readonly name = "OperatorError";
    constructor(details: OperatorErrorDetails);
}
/**
 * One serial connection to a Rust-configured reimport campaign.
 *
 * This surface has no application invocation, remote endpoint, TLS, bearer
 * credential, manifest selection, or raw import method.
 */
export declare class OperatorTransport {
    #private;
    private constructor();
    static connect(socketPath: string, identity: OperatorIdentity): Promise<OperatorTransport>;
    start(canonicalExportManifestJson: string, canonicalExportReceiptJson: string, maximumAttempts?: number): Promise<ReimportOperation>;
    applyPage(page: ReimportPage): Promise<ReimportOperation>;
    status(): Promise<ReimportOperation | undefined>;
    cancel(maximumAttempts?: number): Promise<ReimportOperation | undefined>;
    shutdown(): Promise<void>;
}
