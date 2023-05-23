/* eslint-disable @typescript-eslint/no-explicit-any */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Static, TSchema } from '@sinclair/typebox';

export type ValidKeys<Schema extends TSchema, S = Static<Schema>> = {
    [K in keyof S]-?: S[K] extends string | number ? K : never;
}[keyof S];

export type Keys<Schema extends TSchema> = Readonly<[ValidKeys<Schema>, ValidKeys<Schema>?]> extends Readonly<[string, string?]>
    ? Readonly<[ValidKeys<Schema>, ValidKeys<Schema>?]>
    : never;

export type KeysToObj<Schema extends TSchema, K extends Keys<Schema>, PkOnly = false> = {
    [k in NonNullable<K[PkOnly extends true ? 0 : number]>]: Static<Schema>[k];
};

export type DistKeys<T> = T extends any ? keyof T : never;

export type ValidGsiKeys<S> = DistKeys<S>;
export type GsiKeys<S> = Readonly<[ValidGsiKeys<S>, ValidGsiKeys<S>?]>;
export type GsiKeysToObj<S, K extends GsiKeys<S>> = {
    [k in NonNullable<K[number]>]: S[k];
};

export type CommonKeys<T extends object> = keyof T;
export type AllKeys<T> = T extends any ? keyof T : never;
export type Subtract<A, C> = A extends C ? never : A;
export type NonCommonKeys<T extends object> = Subtract<AllKeys<T>, CommonKeys<T>>;
export type PickType<T, K extends AllKeys<T>> = T extends { [k in K]?: any } ? T[K] : undefined;
export type PickTypeOf<T, K extends string | number | symbol> = K extends AllKeys<T> ? PickType<T, K> : never;

export type Merge<T extends object> = {
    [k in CommonKeys<T>]: PickTypeOf<T, k>;
} & {
    [k in NonCommonKeys<T>]?: PickTypeOf<T, k>;
};

export interface Gsi<Schema extends TSchema = TSchema> {
    schema: Schema;
    keys: GsiKeys<Static<Schema>>;
}

export type InputTransformer<Schema extends TSchema, I = Static<Schema>> = (input: I) => Static<Schema>;
export type OutputTransformer<Schema extends TSchema, O = Static<Schema>> = (output: Static<Schema>) => O;

export interface DdbRepositoryConfig<Schema extends TSchema = TSchema, I = Static<Schema>, O = Static<Schema>> {
    client: DynamoDBClient;
    tableName: string;
    keys: Keys<Schema>;
    validate?: boolean;
    transformInput?: InputTransformer<Schema, I>;
    transformOutput?: OutputTransformer<Schema, O>;
    gsis?: Record<string, Gsi<Schema>>;
}
