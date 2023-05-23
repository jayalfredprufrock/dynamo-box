import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Static, TSchema } from '@sinclair/typebox';
import { and, attributeNotExists, Dynamon, equal, ExpressionSpec, set, update } from '@typemon/dynamon';
import { isExpressionSpec } from '@typemon/dynamon/dist/expression-spec';

import { DdbRepositoryConfig, Gsi, GsiKeysToObj, InputTransformer, KeysToObj, Merge, OutputTransformer } from './types.js';
import { removeUndefined } from './util.js';

export const makeDdbRepository =
    <Schema extends TSchema>(_schema: Schema) =>
    <C extends DdbRepositoryConfig<Schema>>(config: C) => {
        abstract class DdbRepository<
            GSIs = C['gsis'],
            KeysObj extends object = KeysToObj<Schema, C['keys']>,
            Input = C['transformInput'] extends InputTransformer<Schema> ? Parameters<C['transformInput']>[0] : Static<Schema>,
            Output = C['transformOutput'] extends OutputTransformer<Schema> ? ReturnType<C['transformOutput']> : Static<Schema>
        > {
            readonly client: DynamoDBClient = config.client;
            readonly db = new Dynamon(config.client);

            async get(keys: KeysObj, options?: Omit<Dynamon.Get, 'tableName' | 'primaryKey'>): Promise<Output | undefined> {
                const item = await this.db.get({
                    tableName: config.tableName,
                    primaryKey: keys,
                    ...options,
                });

                return item !== undefined ? config.transformOutput?.(item) ?? item : undefined;
            }

            async scan(options?: Omit<Dynamon.Scan, 'tableName'>): Promise<Output[]> {
                const items = await this.db.scanAll({
                    tableName: config.tableName,
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
                    tableName: config.tableName,
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

                const keyConditionExpressionSpec = and(...Object.keys(removeUndefined(keys)).map(k => equal(k, (keys as any)[k])));

                const items = await this.db.queryAll({
                    tableName: config.tableName,
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

            async put(data: Input, options?: Omit<Dynamon.Put, 'tableName' | 'item'>): Promise<Output> {
                const item = removeUndefined(config.transformInput?.(data) ?? data);

                await this.db.put({
                    tableName: config.tableName,
                    item,
                    ...options,
                });

                return config.transformOutput?.(item) ?? item;
            }

            async update(
                keys: KeysObj,
                dataOrExpression: ExpressionSpec | Partial<Omit<Static<Schema>, NonNullable<C['keys'][number]>>>,
                options?: Omit<Dynamon.Update, 'tableName' | 'returnValues' | 'updateExpressionSpec'>
            ): Promise<Output> {
                const updateExpressionSpec = isExpressionSpec(dataOrExpression)
                    ? dataOrExpression
                    : update(...Object.keys(removeUndefined(dataOrExpression)).map(k => set(k, (dataOrExpression as any)[k])));

                const updatedItem = await this.db.update({
                    tableName: config.tableName,
                    primaryKey: keys,
                    returnValues: 'ALL_NEW',
                    updateExpressionSpec,
                    ...options,
                });

                return config.transformOutput?.(updatedItem) ?? updatedItem;
            }

            async delete(
                keys: KeysObj,
                options?: Omit<Dynamon.Delete, 'tableName' | 'returnValues' | 'primaryKey'>
            ): Promise<Output | undefined> {
                const deletedItem = await this.db.delete({
                    tableName: config.tableName,
                    returnValues: 'ALL_OLD',
                    primaryKey: keys,
                    ...options,
                });

                return deletedItem !== undefined ? config.transformOutput?.(deletedItem) ?? deletedItem : undefined;
            }

            async batchGet(
                batchKeys: KeysObj[],
                options?: Omit<Dynamon.BatchGet.Operation, 'primaryKeys'>
            ): Promise<{ responses: Static<Schema>[]; unprocessed: KeysObj[] | undefined }> {
                const { responses, unprocessed } = await this.db.batchGet({
                    [config.tableName]: {
                        primaryKeys: batchKeys,
                        ...options,
                    },
                });

                return {
                    responses: (responses[config.tableName] as Static<Schema>[] | undefined) ?? [],
                    unprocessed: unprocessed?.[config.tableName]?.primaryKeys as KeysObj[] | undefined,
                };
            }

            async batchWrite(
                batchOps: ({ type: 'Delete'; primaryKey: KeysObj } | { type: 'Put'; item: Input })[]
            ): Promise<((KeysObj & { type: 'Delete' }) | { type: 'Put'; item: Input })[] | undefined> {
                const response = await this.db.batchWrite({
                    [config.tableName]: batchOps.map(op => {
                        if (op.type === 'Put') {
                            return {
                                type: 'Put',
                                item: removeUndefined(config.transformInput?.(op.item) ?? op.item),
                            };
                        }
                        return op;
                    }),
                });

                if (!response || !response[config.tableName]?.length) return;

                return response[config.tableName] as ((KeysObj & { type: 'Delete' }) | { type: 'Put'; item: Input })[] | undefined;
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
