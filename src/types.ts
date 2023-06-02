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

// literal union of keys in the S that are eligible to be primary hash/range keys
export type ValidPrimaryKeys<S extends TSchema, T = Static<S>> = {
    [K in keyof T]-?: T[K] extends string | number ? K : never;
}[keyof T];

// tuple representing the primary hash and (optional) range key
export type PrimaryKeys<S extends TSchema> = Readonly<[ValidPrimaryKeys<S>, ValidPrimaryKeys<S>?]> extends Readonly<[string, string?]>
    ? Readonly<[ValidPrimaryKeys<S>, ValidPrimaryKeys<S>?]>
    : never;

// literal union of keys in the S that are eligible to be GSI hash/range keys
export type ValidGsiKeys<S extends TSchema, T = Static<S>> = T extends object
    ? {
          [K in keyof Merge<T>]-?: T[K] extends string | number ? K : never;
      }[keyof T]
    : never;

// tuple representing the primary hash and (optional) range key
export type GsiKeys<S extends TSchema> = Readonly<[ValidGsiKeys<S>, ValidGsiKeys<S>?]> extends Readonly<[string, string?]>
    ? Readonly<[ValidGsiKeys<S>, ValidGsiKeys<S>?]>
    : never;

// utility to build object of primary/GSI hash/range key(s) and their values
export type KeysToObj<S extends TSchema, K extends PrimaryKeys<S> | GsiKeys<S>, IncludeRangeKey extends boolean> = {
    [k in NonNullable<K[IncludeRangeKey extends true ? number : 0]>]: Static<S>[k];
};

// captures changes to input based on transformer function
export type InputTransformer<S extends TSchema, I = Static<S>> = (input: I) => Static<S>;
export type Input<S extends TSchema, C extends DdbRepositoryConfig<S>> = C['transformInput'] extends InputTransformer<S>
    ? Parameters<C['transformInput']>[0]
    : Static<S>;

// captures changes to output based on transformer function
export type OutputTransformer<S extends TSchema, O = Static<S>> = (output: Static<S>) => O;
export type Output<S extends TSchema, C extends DdbRepositoryConfig<S>> = C['transformOutput'] extends OutputTransformer<S>
    ? ReturnType<C['transformOutput']>
    : Static<S>;
export type GsiOutput<S extends TSchema, C extends DdbRepositoryConfig<S>, G extends GsiNames<S, C>> = C['gsis'][G] extends Gsi<S>
    ? C['gsis'][G]['schema'] extends TSchema
        ? Static<C['gsis'][G]['schema']>
        : Output<S, C>
    : never;

// GSI index names
export type GsiNames<S extends TSchema, C extends DdbRepositoryConfig<S>> = keyof C['gsis'];

// GSI configuration
export interface Gsi<S extends TSchema = TSchema> {
    schema?: S;
    keys: GsiKeys<S>;
}

// logger function
export type DdbRepositoryLogger<S extends TSchema> = (log: DdbRepositoryLog<S>) => void;

// main configuration type
export interface DdbRepositoryConfig<S extends TSchema = TSchema, I = Static<S>, O = Static<S>> {
    client?: DynamoDBClient;
    tableName?: string;
    validate?: boolean;
    keys: PrimaryKeys<S>;
    transformInput?: InputTransformer<S, I>;
    transformOutput?: OutputTransformer<S, O>;
    gsis?: Record<string, Gsi<S>>;
    logger?: DdbRepositoryLogger<S>;
}

// subset of configuration type that can be configured at runtime via constructor
export type DdbRepositoryRuntimeConfig = Pick<DdbRepositoryConfig, 'client' | 'tableName' | 'validate' | 'logger'>;

/* Operation Types -------------------------------------------------------------------------------------------------------------- */

export type OperationOptions = { log?: boolean };

export type ScanOptions = Omit<Dynamon.Scan, 'tableName' | 'indexName'> & OperationOptions;

export type GetKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = KeysToObj<S, C['keys'], true>;
export type GetOptions = Omit<Dynamon.Get, 'tableName' | 'primaryKey'> & OperationOptions;

export type QueryKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = KeysToObj<S, C['keys'], false>;
export type QueryOptions = Omit<Dynamon.Query, 'tableName' | 'keyConditionExpressionSpec' | 'indexName'> & OperationOptions;

export type CreateOptions = Omit<Dynamon.Put, 'tableName' | 'returnValues' | 'item' | 'conditionExpressionSpec'> & OperationOptions;

export type PutOptions = Omit<Dynamon.Put, 'tableName' | 'item' | 'returnValues'> & OperationOptions;

export type UpdateKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = KeysToObj<S, C['keys'], true>;
export type UpdateData<S extends TSchema, C extends DdbRepositoryConfig<S>> =
    | ExpressionSpec
    | Partial<Omit<Static<S>, NonNullable<C['keys'][number]>>>;
export type UpdateOptions = Omit<Dynamon.Update, 'tableName' | 'returnValues' | 'updateExpressionSpec' | 'primaryKey'> & OperationOptions;

export type DeleteKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>> = KeysToObj<S, C['keys'], true>;
export type DeleteOptions = Omit<Dynamon.Delete, 'tableName' | 'returnValues' | 'primaryKey'> & OperationOptions;

export type QueryGsiKeysObj<S extends TSchema, C extends DdbRepositoryConfig<S>, G extends GsiNames<S, C>> = C['gsis'][G] extends Gsi<S>
    ? KeysToObj<S, C['gsis'][G]['keys'], false> & Partial<KeysToObj<S, C['gsis'][G]['keys'], true>>
    : never;
export type QueryGsiOptions = Omit<Dynamon.Query, 'tableName' | 'keyConditionExpressionSpec' | 'indexName'> & OperationOptions;

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

export type DdbRepositoryLog<S extends TSchema> =
    | DdbRepositoryScanLog
    | DdbRepositoryQueryLog
    | DdbRepositoryGetLog<S>
    | DdbRepositoryPutLog<S>
    | DdbRepositoryDeleteLog<S>
    | DdbRepositoryUpdateLog<S>;
