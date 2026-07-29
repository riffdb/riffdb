export type ApplicationValueSchema = {
    readonly kind: "bool" | "i64" | "u64" | "string" | "uuid" | "enum" | "bytes" | "date" | "timestamp" | "decimal" | "money" | "cursor" | "limit";
} | {
    readonly kind: "optional";
    readonly value: ApplicationValueSchema;
} | {
    readonly kind: "list";
    readonly value: ApplicationValueSchema;
    readonly maximum?: number;
} | {
    readonly kind: "record";
    readonly fields: ReadonlyArray<{
        readonly name: string;
        readonly schema: ApplicationValueSchema;
        readonly wireId?: number;
    }>;
};
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
export declare class CliApplicationTransport {
    private readonly options;
    constructor(options: CliApplicationTransportOptions);
    executeNamedQuery<P, R>(request: NamedQueryRequest<P, R>, options?: QueryOptions): Promise<TypedQueryResult<R>>;
    executeCommand<I, R>(request: CommandRequest<I, R>, attemptBudget: number): Promise<TypedCommandResult<R>>;
    private baseArguments;
    private invoke;
}
export {};
