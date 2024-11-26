/* eslint-disable @typescript-eslint/ban-types */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import type { Static, TSchema } from '@sinclair/typebox';
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

export type PK = string | number;

/* Configuration Types --------------------------------------------------------------------------------------------------------- */

// literal union of keys in the schema that are eligible to be primary hash/range keys
export type ValidPrimaryKeys<S extends TSchema, E = never, T = Static<S>> = Exclude<
    {
        [K in keyof T]-?: K extends string ? (T[K] extends PK ? K : never) : never;
    }[keyof T],
    E
>;

// grabs the names of the primary keys, handling optional sortKey
export type PrimaryKeys<S extends TSchema, C extends DdbRepositoryConfig<S>> =
    | C['partitionKey']
    | (C['sortKey'] extends PK ? C['sortKey'] : never);

// literal union of keys in the schema that are eligible to be GSI hash/range keys
export type ValidGsiKeys<S extends TSchema, E = never, T = Static<S>> = T extends object
    ? Exclude<
          {
              [K in keyof Merge<T>]: K extends string ? (T[K] extends PK | undefined ? K : never) : never;
          }[keyof T],
          E
      >
    : never;

// grabs the names of the GSI keys, handling optional sortKey
export type GsiKeys<S extends TSchema, C extends DdbRepositoryConfig<S>, G extends GsiNames<S, C>> =
    C['gsis'][G] extends Gsi<S>
        ? C['gsis'][G]['partitionKey'] | (C['gsis'][G]['sortKey'] extends PK ? C['gsis'][G]['sortKey'] : never)
        : never;

// captures changes to input based on transformer function
export type InputTransformer<S extends TSchema, I = Static<S>> = (input: I) => Static<S>;
export type Input<S extends TSchema, C extends DdbRepositoryConfig<S>> =
    C['transformInput'] extends InputTransformer<S>
        ? DistPartialSome<Static<S>, Subtract<RequiredKeys<Static<S>>, RequiredKeys<Parameters<C['transformInput']>[0]>>>
        : Static<S>;

// captures changes to output based on transformer function
export type OutputTransformer<S extends TSchema, O = Static<S>> = (output: Static<S>) => O;
export type Output<S extends TSchema, C extends DdbRepositoryConfig<S>> =
    C['transformOutput'] extends OutputTransformer<S> ? ReturnType<C['transformOutput']> : Static<S>;
export type GsiOutput<S extends TSchema, C extends DdbRepositoryConfig<S>, G extends GsiNames<S, C>> =
    C['gsis'][G] extends Gsi<S>
        ? C['gsis'][G]['projection'] extends (keyof Static<S>)[]
            ? DistPick<Static<S>, PrimaryKeys<S, C> | GsiKeys<S, C, G> | C['gsis'][G]['projection'][number]>
            : C['gsis'][G]['projection'] extends 'KEYS'
              ? DistPick<Static<S>, PrimaryKeys<S, C> | GsiKeys<S, C, G>>
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
}

// subset of configuration type that can be configured at runtime via constructor
export type DdbRepositoryRuntimeConfig = Pick<DdbRepositoryConfig, 'client' | 'tableName' | 'validate'>;

export type Assert<S extends TSchema, C extends DdbRepositoryConfig<S>> = ((output: Output<S, C>) => boolean) | Partial<Static<S>>;

/* Operation Types -------------------------------------------------------------------------------------------------------------- */

export type OperationOptions = { log?: boolean };

export type ScanOptions = Omit<Dynamon.Scan, 'tableName' | 'indexName'> & OperationOptions;

export type ScanGsiOptions = Omit<Dynamon.Scan, 'tableName' | 'indexName' | 'consistentRead'> & OperationOptions;

export type GetKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = Pick<Static<S>, PrimaryKeys<S, C>>;
export type GetOptions<S extends TSchema, C extends DdbRepositoryConfig<S>> = Omit<Dynamon.Get, 'tableName' | 'primaryKey'> &
    OperationOptions & { assert?: Assert<S, C> };

export type QueryKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = Pick<Static<S>, C['partitionKey']>;
export type QueryOptions = PartialSome<Omit<Dynamon.Query, 'tableName' | 'indexName'>, 'keyConditionExpressionSpec'> & OperationOptions;

export type CreateOptions = Omit<Dynamon.Put, 'tableName' | 'returnValues' | 'item' | 'conditionExpressionSpec'> & OperationOptions;

export type PutOptions = Omit<Dynamon.Put, 'tableName' | 'item' | 'returnValues'> & OperationOptions;

export type UpdateKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = Pick<Static<S>, PrimaryKeys<S, C>>;
export type UpdateData<S extends TSchema, C extends DdbRepositoryConfig<S>> =
    | ExpressionSpec
    | DistOmit<Partial<Static<S>>, PrimaryKeys<S, C>>;
export type UpdateOptions = Omit<Dynamon.Update, 'tableName' | 'returnValues' | 'updateExpressionSpec' | 'primaryKey'> & OperationOptions;

export type DeleteKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = Pick<Static<S>, PrimaryKeys<S, C>>;
export type DeleteOptions = Omit<Dynamon.Delete, 'tableName' | 'returnValues' | 'primaryKey'> & OperationOptions;

export type QueryGsiKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>, G extends GsiNames<S, C>> =
    C['gsis'][G] extends Gsi<S>
        ? Required<DistPick<Static<S>, C['gsis'][G]['partitionKey']>> & Partial<DistPick<Static<S>, NonNullable<C['gsis'][G]['sortKey']>>>
        : never;
export type QueryGsiOptions = PartialSome<Omit<Dynamon.Query, 'tableName' | 'indexName' | 'consistentRead'>, 'keyConditionExpressionSpec'> &
    OperationOptions;

export type BatchGetOptions<S extends TSchema, C extends DdbRepositoryConfig<S>> = Omit<Dynamon.BatchGet.Operation, 'primaryKeys'> &
    OperationOptions & { assert?: Assert<S, C> };

export type BatchGetOutput<S extends TSchema, C extends DdbRepositoryConfig<S>> = {
    items: Output<S, C>[];
    unprocessed: GetKeysObj<S, C>[] | undefined;
};

export type BatchWriteOps<S extends TSchema, C extends DdbRepositoryConfig<S>> = (
    | { type: 'Delete'; keys: DeleteKeysObj<S, C> }
    | { type: 'Put'; item: Input<S, C> }
)[];
export type BatchWriteOptions = OperationOptions;
export type BatchWriteOutput<S extends TSchema, C extends DdbRepositoryConfig<S>> = (
    | (DeleteKeysObj<S, C> & { type: 'Delete' })
    | { type: 'Put'; item: Output<S, C> }
)[];

export type BatchPutOptions = OperationOptions;

export type BatchDeleteOptions = OperationOptions;

/* Logger Types ----------------------------------------------------------------------------------------------------------------- */

export interface DdbRepositoryOpLogBase {
    time: number;
    duration: number;
}

export interface DdbRepositoryGetLog<S extends TSchema> extends DdbRepositoryOpLogBase {
    operation: 'GET';
    item?: Static<S>;
}

export interface DdbRepositoryScanLog extends DdbRepositoryOpLogBase {
    operation: 'SCAN';
    itemCount: number;
    indexName?: string;
}

export interface DdbRepositoryQueryLog extends DdbRepositoryOpLogBase {
    operation: 'QUERY';
    itemCount: number;
    indexName?: string;
}

export interface DdbRepositoryPutLog<S extends TSchema> extends DdbRepositoryOpLogBase {
    operation: 'PUT';
    item: Static<S>;
    prevItem?: Static<S>;
}

export interface DdbRepositoryDeleteLog<S extends TSchema> extends DdbRepositoryOpLogBase {
    operation: 'DELETE';
    item?: undefined;
    prevItem?: Static<S>;
}

export interface DdbRepositoryUpdateLog<S extends TSchema> extends DdbRepositoryOpLogBase {
    operation: 'UPDATE';
    item: Static<S>;
    prevItem?: Static<S>;
}

export interface DdbRepositoryBatchGetLog extends DdbRepositoryOpLogBase {
    operation: 'BATCH_GET';
    unprocessedCount: number;
}

export interface DdbRepositoryBatchWriteLog extends DdbRepositoryOpLogBase {
    operation: 'BATCH_WRITE';
    unprocessedCount: number;
}

export type DdbRepositoryOpLogEvent<S extends TSchema> =
    | DdbRepositoryScanLog
    | DdbRepositoryQueryLog
    | DdbRepositoryGetLog<S>
    | DdbRepositoryPutLog<S>
    | DdbRepositoryDeleteLog<S>
    | DdbRepositoryUpdateLog<S>
    | DdbRepositoryBatchGetLog
    | DdbRepositoryBatchWriteLog;

export interface DdbRepositoryWriteEvent<S extends TSchema> {
    time: number;
    operation: 'PUT' | 'UPDATE' | 'BATCH_WRITE';
    item: Static<S>;
}

export type DdbRepositoryEvents<S extends TSchema> = {
    operation: (event: DdbRepositoryOpLogEvent<S>) => void;
    write: (event: DdbRepositoryWriteEvent<S>) => void;
};
