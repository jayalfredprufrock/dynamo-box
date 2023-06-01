/* eslint-disable @typescript-eslint/ban-types */
/* eslint-disable @typescript-eslint/no-explicit-any */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Static, TSchema } from '@sinclair/typebox';
import { Dynamon, ExpressionSpec } from '@typemon/dynamon';

export type ValidKeys<Schema extends TSchema, S = Static<Schema>> = {
    [K in keyof S]-?: S[K] extends string | number ? K : never;
}[keyof S];

export type Keys<Schema extends TSchema> = Readonly<[ValidKeys<Schema>, ValidKeys<Schema>?]> extends Readonly<[string, string?]>
    ? Readonly<[ValidKeys<Schema>, ValidKeys<Schema>?]>
    : never;

export type KeysToObj<Schema extends TSchema, K extends Keys<Schema>, PkOnly = false> = {
    [k in NonNullable<K[PkOnly extends true ? 0 : number]>]: Static<Schema>[k];
};

export type ValidGsiKeys<S> = DistKeys<S>;
export type Gsis<S> = Readonly<[ValidGsiKeys<S>, ValidGsiKeys<S>?]>;
export type GsiKeysToObj<S, K extends Gsis<S>> = {
    [k in NonNullable<K[number]>]: S[k];
};

export type CommonKeys<T extends object> = keyof T;
export type AllKeys<T> = T extends any ? keyof T : never;
export type Subtract<A, C> = A extends C ? never : A;
export type NonCommonKeys<T extends object> = Subtract<AllKeys<T>, CommonKeys<T>>;
export type PickType<T, K extends AllKeys<T>> = T extends { [k in K]?: any } ? T[K] : undefined;
export type PickTypeOf<T, K extends string | number | symbol> = K extends AllKeys<T> ? PickType<T, K> : never;

// Distributive variants primarly for union support
export type FlattenType<T> = { [KeyType in keyof T]: T[KeyType] } & {};

export type PartialSome<T, K extends keyof T> = FlattenType<Omit<T, K> & Partial<Pick<T, K>>>;
export type RequiredSome<T, K extends keyof T> = FlattenType<Required<Pick<T, K>> & Omit<T, K>>;
export type DistKeys<T> = T extends unknown ? keyof T : never;
export type DistPick<T, K extends keyof T> = T extends unknown ? Pick<T, K> : never;
export type DistOmit<T, K extends keyof T> = T extends unknown ? Omit<T, K> : never;
export type DistPartialSome<T, K extends keyof T> = T extends unknown ? PartialSome<T, K> : never;
export type DistRequiredSome<T, K extends keyof T> = T extends unknown ? RequiredSome<T, K> : never;

export type Merge<T extends object> = {
    [k in CommonKeys<T>]: PickTypeOf<T, k>;
} & {
    [k in NonCommonKeys<T>]?: PickTypeOf<T, k>;
};

export interface Gsi<Schema extends TSchema = TSchema> {
    schema: Schema;
    keys: Gsis<Static<Schema>>;
}

export type InputTransformer<Schema extends TSchema, I = Static<Schema>> = (input: I) => Static<Schema>;
export type OutputTransformer<Schema extends TSchema, O = Static<Schema>> = (output: Static<Schema>) => O;

export type Input<Schema extends TSchema, C extends DdbRepositoryConfig<Schema>> = C['transformInput'] extends InputTransformer<Schema>
    ? Parameters<C['transformInput']>[0]
    : Static<Schema>;

export type Output<Schema extends TSchema, C extends DdbRepositoryConfig<Schema>> = C['transformOutput'] extends OutputTransformer<Schema>
    ? ReturnType<C['transformOutput']>
    : Static<Schema>;

export type GsiKeys<Schema extends TSchema, C extends DdbRepositoryConfig<Schema>> = keyof C['gsis'];
export type KeysObj<Schema extends TSchema, C extends DdbRepositoryConfig<Schema>> = KeysToObj<Schema, C['keys']>;
export type PrimaryKeyObj<Schema extends TSchema, C extends DdbRepositoryConfig<Schema>> = KeysToObj<Schema, C['keys'], true>;
export type GsiKeysObj<
    Schema extends TSchema,
    C extends DdbRepositoryConfig<Schema>,
    G extends keyof C['gsis']
> = C['gsis'][G] extends Gsi<Schema>
    ? GsiKeysToObj<Static<C['gsis'][G]['schema']> extends object ? Merge<Static<C['gsis'][G]['schema']>> : never, C['gsis'][G]['keys']>
    : never;

export type ScanOptions = Omit<Dynamon.Scan, 'tableName'> & { log?: boolean };
export type GetOptions = Omit<Dynamon.Get, 'tableName' | 'primaryKey'> & { log?: boolean };
export type QueryOptions = Omit<Dynamon.Query, 'tableName' | 'keyConditionExpressionSpec'> & { log?: boolean };
export type CreateOptions = Omit<Dynamon.Put, 'tableName' | 'returnValues' | 'item' | 'conditionExpressionSpec'> & { log?: boolean };
export type PutOptions = Omit<Dynamon.Put, 'tableName' | 'item' | 'returnValues'> & { log?: boolean };
export type UpdateData<Schema extends TSchema, C extends DdbRepositoryConfig<Schema>> =
    | ExpressionSpec
    | Partial<Omit<Static<Schema>, NonNullable<C['keys'][number]>>>;
export type UpdateOptions = Omit<Dynamon.Update, 'tableName' | 'returnValues' | 'updateExpressionSpec'> & { log?: boolean };
export type DeleteOptions = Omit<Dynamon.Delete, 'tableName' | 'returnValues' | 'primaryKey'> & { log?: boolean };

export interface DdbRepositoryLogBase {
    time: number;
    duration: number;
}

export interface DdbRepositoryGetLog<Schema extends TSchema> extends DdbRepositoryLogBase {
    operation: 'GET';
    item?: Static<Schema>;
}

export interface DdbRepositoryScanLog extends DdbRepositoryLogBase {
    operation: 'SCAN';
    itemCount: number;
    gsi?: string;
}

export interface DdbRepositoryQueryLog extends DdbRepositoryLogBase {
    operation: 'QUERY';
    itemCount: number;
    gsi?: string;
}

export interface DdbRepositoryPutLog<Schema extends TSchema> extends DdbRepositoryLogBase {
    operation: 'PUT';
    item: Static<Schema>;
    prevItem?: Static<Schema>;
}

export interface DdbRepositoryDeleteLog<Schema extends TSchema> extends DdbRepositoryLogBase {
    operation: 'DELETE';
    prevItem?: Static<Schema>;
}

export interface DdbRepositoryUpdateLog<Schema extends TSchema> extends DdbRepositoryLogBase {
    operation: 'UPDATE';
    item: Static<Schema>;
    prevItem?: Static<Schema>;
}

export type DdbRepositoryLog<Schema extends TSchema> =
    | DdbRepositoryScanLog
    | DdbRepositoryQueryLog
    | DdbRepositoryGetLog<Schema>
    | DdbRepositoryPutLog<Schema>
    | DdbRepositoryDeleteLog<Schema>
    | DdbRepositoryUpdateLog<Schema>;

export type DdbRepositoryLogger<Schema extends TSchema> = (log: DdbRepositoryLog<Schema>) => void;

export interface DdbRepositoryConfig<Schema extends TSchema = TSchema, I = Static<Schema>, O = Static<Schema>> {
    client?: DynamoDBClient;
    tableName?: string;
    validate?: boolean;
    keys: Keys<Schema>;
    transformInput?: InputTransformer<Schema, I>;
    transformOutput?: OutputTransformer<Schema, O>;
    gsis?: Record<string, Gsi<Schema>>;
    logger?: DdbRepositoryLogger<Schema>;
}

export type DdbRepositoryRuntimeConfig = Pick<DdbRepositoryConfig, 'client' | 'tableName' | 'validate' | 'logger'>;
