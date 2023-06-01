import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Static, TSchema } from '@sinclair/typebox';
import { and, attributeNotExists, Dynamon, equal, ExpressionSpec, set, update } from '@typemon/dynamon';
import { isExpressionSpec } from '@typemon/dynamon/dist/expression-spec';

import {
    DdbRepositoryConfig,
    DdbRepositoryLogger,
    DdbRepositoryRuntimeConfig,
    Gsi,
    GsiKeysToObj,
    InputTransformer,
    KeysToObj,
    Merge,
    OutputTransformer,
} from './types.js';
import { hrTimeMs, removeUndefined } from './util.js';

export const makeDdbRepository =
    <Schema extends TSchema>(schema: Schema) =>
    <C extends DdbRepositoryConfig<Schema>>(config: C) => {
        abstract class DdbRepository<
            GSIs = C['gsis'],
            KeysObj extends object = KeysToObj<Schema, C['keys']>,
            Input = C['transformInput'] extends InputTransformer<Schema> ? Parameters<C['transformInput']>[0] : Static<Schema>,
            Output = C['transformOutput'] extends OutputTransformer<Schema> ? ReturnType<C['transformOutput']> : Static<Schema>
        > {
            readonly schema: Schema = schema;
            readonly client: DynamoDBClient;
            readonly db: Dynamon;
            readonly tableName: string;
            readonly validate: boolean;

            readonly logger?: DdbRepositoryLogger<Schema>;

            constructor(runtimeConfig?: DdbRepositoryRuntimeConfig) {
                const client = runtimeConfig?.client ?? config.client;
                if (!client) {
                    throw new Error('Client must be passed in "makeDdbRepository" config or within the constructor.');
                }
                this.client = client;
                this.db = new Dynamon(this.client);

                const tableName = runtimeConfig?.tableName ?? config?.tableName;
                if (!tableName) {
                    throw new Error('Table name must be passed in "makeDdbRepository" config or within the constructor.');
                }

                this.tableName = tableName;
                this.validate = runtimeConfig?.validate ?? config?.validate ?? false;
                this.logger = runtimeConfig?.logger ?? config?.logger;
            }

            async get(keys: KeysObj, options?: Omit<Dynamon.Get, 'tableName' | 'primaryKey'>): Promise<Output | undefined> {
                const item = await this.db.get({
                    tableName: this.tableName,
                    primaryKey: keys,
                    ...options,
                });

                return item !== undefined ? config.transformOutput?.(item) ?? item : undefined;
            }

            async scan(options?: Omit<Dynamon.Scan, 'tableName'>): Promise<Output[]> {
                const items = await this.db.scanAll({
                    tableName: this.tableName,
                    ...options,
                });

                if (!config.transformOutput) {
                    return items as Output[];
                }

                return items.map(config.transformOutput) as Output[];
            }

            async query(
                keys: KeysToObj<Schema, C['keys'], true>,
                options?: Omit<Dynamon.Query, 'tableName' | 'keyConditionExpressionSpec'>
            ): Promise<Output[]> {
                const items = await this.db.queryAll({
                    tableName: this.tableName,
                    keyConditionExpressionSpec: equal(config.keys[0], keys[config.keys[0]]),
                    ...options,
                });

                if (!config.transformOutput) {
                    return items as Output[];
                }

                return items.map(config.transformOutput) as Output[];
            }

            async queryGsi<G extends keyof GSIs>(
                name: G,
                keys: GSIs[G] extends Gsi<Schema>
                    ? GsiKeysToObj<Static<GSIs[G]['schema']> extends object ? Merge<Static<GSIs[G]['schema']>> : never, GSIs[G]['keys']>
                    : never,
                options?: Omit<Dynamon.Query, 'tableName' | 'keyConditionExpressionSpec'>
            ): Promise<Output[]> {
                const gsi = config.gsis?.[String(name)];
                if (!gsi) {
                    throw new Error(`Unrecognized GSI "${String(name)}"`);
                }

                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const keyConditionExpressionSpec = and(...Object.keys(removeUndefined(keys)).map(k => equal(k, (keys as any)[k])));

                const items = await this.db.queryAll({
                    tableName: this.tableName,
                    keyConditionExpressionSpec,
                    ...options,
                });

                if (!config.transformOutput) {
                    return items as Output[];
                }

                return items.map(config.transformOutput) as Output[];
            }

            async create(
                data: Input,
                options?: Omit<Dynamon.Put, 'tableName' | 'returnValues' | 'item' | 'conditionExpressionSpec'>
            ): Promise<Output> {
                const item = removeUndefined(config.transformInput?.(data) ?? data);
                const conditionExpressionSpec = and(
                    ...config.keys.filter((key): key is string => !!key).map(key => attributeNotExists(key))
                );
                return this.put(item, { ...options, conditionExpressionSpec });
            }

            async put(data: Input, options?: Omit<Dynamon.Put, 'tableName' | 'item' | 'returnValues'>): Promise<Output> {
                const time = hrTimeMs();

                const item = removeUndefined(config.transformInput?.(data) ?? data);

                const prevItem = await this.db.put({
                    tableName: this.tableName,
                    item,
                    returnValues: 'ALL_OLD',
                    ...options,
                });

                const output = config.transformOutput?.(item) ?? item;

                this.logger?.({
                    operation: 'PUT',
                    time,
                    duration: hrTimeMs() - time,
                    item,
                    prevItem,
                });

                return output;
            }

            async update(
                keys: KeysObj,
                dataOrExpression: ExpressionSpec | Partial<Omit<Static<Schema>, NonNullable<C['keys'][number]>>>,
                options?: Omit<Dynamon.Update, 'tableName' | 'returnValues' | 'updateExpressionSpec'>
            ): Promise<Output> {
                const time = hrTimeMs();

                const updateExpressionSpec = isExpressionSpec(dataOrExpression)
                    ? dataOrExpression
                    : // eslint-disable-next-line @typescript-eslint/no-explicit-any
                      update(...Object.keys(removeUndefined(dataOrExpression)).map(k => set(k, (dataOrExpression as any)[k])));

                const item = await this.db.update({
                    tableName: this.tableName,
                    primaryKey: keys,
                    returnValues: 'ALL_NEW',
                    updateExpressionSpec,
                    ...options,
                });

                const output = config.transformOutput?.(item) ?? item;

                this.logger?.({
                    operation: 'UPDATE',
                    time,
                    duration: hrTimeMs() - time,
                    item,
                });

                return output;
            }

            async delete(
                keys: KeysObj,
                options?: Omit<Dynamon.Delete, 'tableName' | 'returnValues' | 'primaryKey'>
            ): Promise<Output | undefined> {
                const time = hrTimeMs();

                const prevItem = await this.db.delete({
                    tableName: this.tableName,
                    returnValues: 'ALL_OLD',
                    primaryKey: keys,
                    ...options,
                });

                const prevOutput = prevItem !== undefined ? config.transformOutput?.(prevItem) ?? prevItem : undefined;

                this.logger?.({
                    operation: 'DELETE',
                    time,
                    duration: hrTimeMs() - time,
                    prevItem,
                });

                return prevOutput;
            }

            async batchGet(
                batchKeys: KeysObj[],
                options?: Omit<Dynamon.BatchGet.Operation, 'primaryKeys'>
            ): Promise<{ responses: Static<Schema>[]; unprocessed: KeysObj[] | undefined }> {
                const { responses, unprocessed } = await this.db.batchGet({
                    [this.tableName]: {
                        primaryKeys: batchKeys,
                        ...options,
                    },
                });

                return {
                    responses: (responses[this.tableName] as Static<Schema>[] | undefined) ?? [],
                    unprocessed: unprocessed?.[this.tableName]?.primaryKeys as KeysObj[] | undefined,
                };
            }

            async batchWrite(
                batchOps: ({ type: 'Delete'; primaryKey: KeysObj } | { type: 'Put'; item: Input })[]
            ): Promise<((KeysObj & { type: 'Delete' }) | { type: 'Put'; item: Input })[] | undefined> {
                const response = await this.db.batchWrite({
                    [this.tableName]: batchOps.map(op => {
                        if (op.type === 'Put') {
                            return {
                                type: 'Put',
                                item: removeUndefined(config.transformInput?.(op.item) ?? op.item),
                            };
                        }
                        return op;
                    }),
                });

                if (!response || !response[this.tableName]?.length) return;

                return response[this.tableName] as ((KeysObj & { type: 'Delete' }) | { type: 'Put'; item: Input })[] | undefined;
            }

            async batchDelete(batchKeys: KeysObj[]): Promise<KeysObj[] | undefined> {
                const response = await this.batchWrite(batchKeys.map(primaryKey => ({ type: 'Delete', primaryKey })));
                if (!response || !response.length) return;

                return response.map(op => (op as Dynamon.BatchWrite.Delete).primaryKey) as KeysObj[];
            }

            async batchPut(batchItems: Input[]): Promise<Input[] | undefined> {
                const response = await this.batchWrite(batchItems.map(item => ({ type: 'Put', item })));
                if (!response || !response.length) return;

                return response.map(op => (op as Dynamon.BatchWrite.Put).item) as Input[];
            }
        }

        return DdbRepository;
    };
