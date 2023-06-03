/* eslint-disable @typescript-eslint/ban-types */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Static, TSchema } from '@sinclair/typebox';
import { Dynamon, ExpressionSpec } from '@typemon/dynamon';

/* Utility Types ------------------------------------------------------------------------------------------------------------- */
export type CommonKeys<T extends object> = keyof T;
export type AllKeys<T> = T extends any ? keyof T : never;
export type Subtract<A, C> = A extends C ? never : A;
export type NonCommonKeys<T extends object> = Subtract<AllKeys<T>, CommonKeys<T>>;
export type PickType<T, K extends AllKeys<T>> = T extends { [k in K]?: any } ? T[K] : undefined;
export type PickTypeOf<T, K extends string | number | symbol> = K extends AllKeys<T> ? PickType<T, K> : never;
export type FlattenType<T> = { [KeyType in keyof T]: T[KeyType] } & {};
export type PartialSome<T, K extends keyof T> = FlattenType<Omit<T, K> & Partial<Pick<T, K>>>;
export type RequiredSome<T, K extends keyof T> = FlattenType<Required<Pick<T, K>> & Omit<T, K>>;
export type RequiredKeys<T> = { [K in keyof T]-?: {} extends Pick<T, K> ? never : K }[keyof T];
export type Merge<T extends object> = {
    [k in CommonKeys<T>]: PickTypeOf<T, k>;
} & {
    [k in NonCommonKeys<T>]?: PickTypeOf<T, k>;
};

// Distributive variants primarly for union support
export type DistKeys<T> = T extends unknown ? keyof T : never;
export type DistPick<T, K extends keyof T> = T extends unknown ? Pick<T, K> : never;
export type DistOmit<T, K extends keyof T> = T extends unknown ? Omit<T, K> : never;
export type DistPartialSome<T, K extends keyof T> = T extends unknown ? PartialSome<T, K> : never;
export type DistRequiredSome<T, K extends keyof T> = T extends unknown ? RequiredSome<T, K> : never;

/* Configuration Types --------------------------------------------------------------------------------------------------------- */

// literal union of keys in the schema that are eligible to be primary hash/range keys
export type ValidPrimaryKeys<S extends TSchema, E = never, T = Static<S>> = Exclude<
    {
        [K in keyof T]-?: K extends string ? (T[K] extends string | number ? K : never) : never;
    }[keyof T],
    E
>;

// literal union of keys in the schema that are eligible to be GSI hash/range keys
export type ValidGsiKeys<S extends TSchema, E = never, T = Static<S>> = T extends object
    ? Exclude<
          {
              [K in keyof Merge<T>]: K extends string ? (T[K] extends string | number | undefined ? K : never) : never;
          }[keyof T],
          E
      >
    : never;

// captures changes to input based on transformer function
export type InputTransformer<S extends TSchema, I = Static<S>> = (input: I) => Static<S>;
export type Input<S extends TSchema, C extends DdbRepositoryConfig<S>> = C['transformInput'] extends InputTransformer<S>
    ? DistPartialSome<Static<S>, Subtract<RequiredKeys<Static<S>>, RequiredKeys<Parameters<C['transformInput']>[0]>>>
    : Static<S>;

// captures changes to output based on transformer function
export type OutputTransformer<S extends TSchema, O = Static<S>> = (output: Static<S>) => O;
export type Output<S extends TSchema, C extends DdbRepositoryConfig<S>> = C['transformOutput'] extends OutputTransformer<S>
    ? ReturnType<C['transformOutput']>
    : Static<S>;
export type GsiOutput<S extends TSchema, C extends DdbRepositoryConfig<S>, G extends GsiNames<S, C>> = C['gsis'][G] extends Gsi<S>
    ? C['gsis'][G]['projection'] extends (keyof Static<S>)[]
        ? Pick<
              Static<S>,
              | C['partitionKey']
              | NonNullable<C['sortKey']>
              | C['gsis'][G]['partitionKey']
              | NonNullable<C['gsis'][G]['sortKey']>
              | C['gsis'][G]['projection'][number]
          >
        : C['gsis'][G]['projection'] extends 'KEYS'
        ? Pick<
              Static<S>,
              C['partitionKey'] | NonNullable<C['sortKey']> | C['gsis'][G]['partitionKey'] | NonNullable<C['gsis'][G]['sortKey']>
          >
        : Output<S, C>
    : never;

// GSI index names
export type GsiNames<S extends TSchema, C extends DdbRepositoryConfig<S>> = Extract<keyof C['gsis'], string>;

// GSI configuration
export interface Gsi<S extends TSchema> {
    partitionKey: ValidGsiKeys<S>;
    sortKey?: ValidGsiKeys<S>;
    projection?: 'ALL' | 'KEYS' | AllKeys<Static<S>>[];
}

// logger function
export type DdbRepositoryLogger<S extends TSchema> = (log: DdbRepositoryLog<S>) => void;

// main configuration type
export interface DdbRepositoryConfig<S extends TSchema = TSchema> {
    client?: DynamoDBClient;
    tableName?: string;
    validate?: boolean;
    partitionKey: ValidPrimaryKeys<S>;
    sortKey?: ValidPrimaryKeys<S>;
    transformInput?: InputTransformer<S>;
    transformOutput?: OutputTransformer<S>;
    gsis?: Record<string, Gsi<S>>;
    logger?: DdbRepositoryLogger<S>;
}

// subset of configuration type that can be configured at runtime via constructor
export type DdbRepositoryRuntimeConfig = Pick<DdbRepositoryConfig, 'client' | 'tableName' | 'validate' | 'logger'>;

/* Operation Types -------------------------------------------------------------------------------------------------------------- */

export type OperationOptions = { log?: boolean };

export type ScanOptions = Omit<Dynamon.Scan, 'tableName' | 'indexName'> & OperationOptions;

export type GetKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = Pick<
    Static<S>,
    NonNullable<C['partitionKey'] | C['sortKey']>
>;
export type GetOptions = Omit<Dynamon.Get, 'tableName' | 'primaryKey'> & OperationOptions;

export type QueryKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = Pick<Static<S>, C['partitionKey']>;
export type QueryOptions = Omit<Dynamon.Query, 'tableName' | 'keyConditionExpressionSpec' | 'indexName'> & OperationOptions;

export type CreateOptions = Omit<Dynamon.Put, 'tableName' | 'returnValues' | 'item' | 'conditionExpressionSpec'> & OperationOptions;

export type PutOptions = Omit<Dynamon.Put, 'tableName' | 'item' | 'returnValues'> & OperationOptions;

export type UpdateKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = Pick<
    Static<S>,
    NonNullable<C['partitionKey'] | C['sortKey']>
>;
export type UpdateData<S extends TSchema, C extends DdbRepositoryConfig<S>> =
    | ExpressionSpec
    | Partial<Omit<Static<S>, NonNullable<C['partitionKey'] | C['sortKey']>>>;
export type UpdateOptions = Omit<Dynamon.Update, 'tableName' | 'returnValues' | 'updateExpressionSpec' | 'primaryKey'> & OperationOptions;

export type DeleteKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = Pick<
    Static<S>,
    NonNullable<C['partitionKey'] | C['sortKey']>
>;
export type DeleteOptions = Omit<Dynamon.Delete, 'tableName' | 'returnValues' | 'primaryKey'> & OperationOptions;

export type QueryGsiKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>, G extends GsiNames<S, C>> = C['gsis'][G] extends Gsi<S>
    ? Required<Pick<Static<S>, C['gsis'][G]['partitionKey']>> & Partial<Pick<Static<S>, NonNullable<C['gsis'][G]['sortKey']>>>
    : never;
export type QueryGsiOptions = Omit<Dynamon.Query, 'tableName' | 'keyConditionExpressionSpec' | 'indexName'> & OperationOptions;

export type BatchGetOptions = Omit<Dynamon.BatchGet.Operation, 'primaryKeys'> & OperationOptions;
export type BatchGetOutput<S extends TSchema, C extends DdbRepositoryConfig<S>> = {
    items: Output<S, C>[];
    unprocessed: GetKeysObj<S, C>[] | undefined;
};

export type BatchWriteOps<S extends TSchema, C extends DdbRepositoryConfig<S>> = (
    | { type: 'Delete'; keys: DeleteKeysObj<S, C> }
    | { type: 'Put'; item: Input<S, C> }
)[];
export type BatchWriteOptions = OperationOptions;
export type BatchWriteOutput<S extends TSchema, C extends DdbRepositoryConfig<S>> =
    | ((DeleteKeysObj<S, C> & { type: 'Delete' }) | { type: 'Put'; item: Input<S, C> })[]
    | undefined;

export type BatchPutOptions = OperationOptions;

export type BatchDeleteOptions = OperationOptions;

/* Logger Types ----------------------------------------------------------------------------------------------------------------- */

export interface DdbRepositoryLogBase {
    time: number;
    duration: number;
}

export interface DdbRepositoryGetLog<S extends TSchema> extends DdbRepositoryLogBase {
    operation: 'GET';
    item?: Static<S>;
}

export interface DdbRepositoryScanLog extends DdbRepositoryLogBase {
    operation: 'SCAN';
    itemCount: number;
    indexName?: string;
}

export interface DdbRepositoryQueryLog extends DdbRepositoryLogBase {
    operation: 'QUERY';
    itemCount: number;
    indexName?: string;
}

export interface DdbRepositoryPutLog<S extends TSchema> extends DdbRepositoryLogBase {
    operation: 'PUT';
    item: Static<S>;
    prevItem?: Static<S>;
}

export interface DdbRepositoryDeleteLog<S extends TSchema> extends DdbRepositoryLogBase {
    operation: 'DELETE';
    item?: undefined;
    prevItem?: Static<S>;
}

export interface DdbRepositoryUpdateLog<S extends TSchema> extends DdbRepositoryLogBase {
    operation: 'UPDATE';
    item: Static<S>;
    prevItem?: Static<S>;
}

export interface DdbRepositoryBatchGetLog extends DdbRepositoryLogBase {
    operation: 'BATCH_GET';
    unprocessedCount: number;
}

export interface DdbRepositoryBatchWriteLog extends DdbRepositoryLogBase {
    operation: 'BATCH_WRITE';
    unprocessedCount: number;
}

export type DdbRepositoryLog<S extends TSchema> =
    | DdbRepositoryScanLog
    | DdbRepositoryQueryLog
    | DdbRepositoryGetLog<S>
    | DdbRepositoryPutLog<S>
    | DdbRepositoryDeleteLog<S>
    | DdbRepositoryUpdateLog<S>
    | DdbRepositoryBatchGetLog
    | DdbRepositoryBatchWriteLog;
