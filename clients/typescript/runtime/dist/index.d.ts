export type ApplicationValueSchema = {
    readonly kind: "bool" | "i64" | "u64" | "string" | "uuid" | "bytes" | "date" | "timestamp" | "cursor" | "limit";
} | {
    readonly kind: "decimal";
    readonly precision?: number;
    readonly scale?: number;
} | {
    readonly kind: "money";
    readonly precision?: number;
    readonly scale?: number;
    readonly currency?: string;
} | {
    readonly kind: "enum";
    readonly typeId?: number;
    readonly variants?: Readonly<Record<string, number>>;
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
export declare function exactDecimal(value: string, precision: number, scale: number): ExactDecimalValue;
/** Constructs the exact precision-38, scale-2 value used by `money<CURRENCY>`. */
export declare function exactMoney<const Currency extends string>(currency: Currency, value: string): ExactMoneyValue<Currency>;
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
export type ReactiveEventMutationResult = "applied" | "state_changed" | "not_found" | "outstanding_lease" | "stale_lease" | "lease_expired";
export type LiveQueryPatchOperation = {
    readonly type: "insert";
    readonly index: number;
    readonly record: Readonly<Record<string, unknown>>;
} | {
    readonly type: "remove";
    readonly index: number;
    readonly key: Readonly<Record<string, unknown>>;
} | {
    readonly type: "replace";
    readonly index: number;
    readonly record: Readonly<Record<string, unknown>>;
} | {
    readonly type: "move";
    readonly from: number;
    readonly to: number;
    readonly key: Readonly<Record<string, unknown>>;
};
export type LiveQueryUpdate<T> = {
    readonly type: "snapshot";
    readonly value: T;
    readonly cursor: string;
    readonly applicationHead: bigint;
    readonly historyIncarnation: bigint;
} | {
    readonly type: "patch";
    readonly resultField: string;
    readonly operations: ReadonlyArray<LiveQueryPatchOperation>;
    readonly cursor: string;
    readonly applicationHead: bigint;
    readonly historyIncarnation: bigint;
} | {
    readonly type: "reset";
    readonly reason: string;
    readonly value: T;
    readonly cursor: string;
    readonly applicationHead: bigint;
    readonly historyIncarnation: bigint;
} | {
    readonly type: "checkpoint";
    readonly cursor: string;
    readonly applicationHead: bigint;
    readonly historyIncarnation: bigint;
} | {
    readonly type: "terminal";
    readonly reason: string;
    readonly lastApplicationHead?: bigint;
    readonly historyIncarnation?: bigint;
};
export declare class CliApplicationTransport {
    private readonly options;
    constructor(options: CliApplicationTransportOptions);
    executeNamedQuery<P, R>(request: NamedQueryRequest<P, R>, options?: QueryOptions): Promise<TypedQueryResult<R>>;
    executeCommand<I, R>(request: CommandRequest<I, R>, attemptBudget: number): Promise<TypedCommandResult<R>>;
    consumeEventStream<P, E>(request: ReactiveConsumerRequest<P>, options?: ReactiveConsumerOptions): AsyncIterable<ReactiveConsumerBatch<E>>;
    acknowledgeEvent<P>(request: ReactiveConsumerRequest<P>, delivery: ReactiveEventDelivery<unknown>): Promise<ReactiveEventMutationResult>;
    negativeAcknowledgeEvent<P>(request: ReactiveConsumerRequest<P>, delivery: ReactiveEventDelivery<unknown>, retryDelayMs?: number): Promise<ReactiveEventMutationResult>;
    seekEventConsumer<P>(request: ReactiveConsumerRequest<P>, checkpoint: string): Promise<ReactiveEventMutationResult>;
    eventConsumerStatus<P>(request: ReactiveConsumerRequest<P>): Promise<ReactiveConsumerStatus | undefined>;
    consumeContextualSubscription<P, E>(request: ReactiveConsumerRequest<P>, maximumWaitMs?: number): Promise<ContextualBatch<E>>;
    acknowledgeContextualItem<P>(request: ReactiveConsumerRequest<P>, item: ContextualWorkItem<unknown>): Promise<ReactiveEventMutationResult>;
    negativeAcknowledgeContextualItem<P>(request: ReactiveConsumerRequest<P>, item: ContextualWorkItem<unknown>, retryDelayMs?: number): Promise<ReactiveEventMutationResult>;
    contextualSubscriptionStatus<P>(request: ReactiveConsumerRequest<P>): Promise<ReactiveConsumerStatus | undefined>;
    executeContextualReaction<P, I, R>(request: ReactiveConsumerRequest<P>, reaction: ContextualReaction, command: CommandRequest<I, R>): Promise<TypedCommandResult<R>>;
    watchNamedQuery<P, T>(request: {
        readonly reactiveModuleHash: string;
        readonly operationName: string;
        readonly parameters: P;
        readonly parameterSchema: ApplicationValueSchema;
        readonly cursor?: string;
    }): AsyncIterable<LiveQueryUpdate<T>>;
    private mutateEventLease;
    private mutateContextualItem;
    private reactiveArguments;
    private baseArguments;
    private invoke;
    private invokeArguments;
}
export {};
