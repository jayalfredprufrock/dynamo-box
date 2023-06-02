import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Static, TSchema, Type } from '@sinclair/typebox';
import { and, attributeNotExists, Dynamon, equal, set, update } from '@typemon/dynamon';
import { isExpressionSpec } from '@typemon/dynamon/dist/expression-spec';

import {
    CreateOptions,
    DdbRepositoryConfig,
    DdbRepositoryLogger,
    DdbRepositoryRuntimeConfig,
    DeleteKeysObj,
    DeleteOptions,
    DistOmit,
    GetKeysObj,
    GetOptions,
    GsiNames,
    GsiOutput,
    Input,
    Output,
    PutOptions,
    QueryGsiKeysObj,
    QueryGsiOptions,
    QueryKeysObj,
    QueryOptions,
    ScanOptions,
    UpdateData,
    UpdateKeysObj,
    UpdateOptions,
} from './types.js';
import { hrTimeToMs, removeUndefined } from './util.js';

export const makeDdbRepository =
    <S extends TSchema>(schema: S) =>
    <C extends DdbRepositoryConfig<S>>(config: C) => {
        abstract class DdbRepository {
            readonly schema: S = schema;
            readonly client: DynamoDBClient;
            readonly db: Dynamon;
            readonly tableName: string;
            readonly validate: boolean;

            readonly logger?: DdbRepositoryLogger<S>;

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

            async scan(options?: ScanOptions): Promise<Output<S, C>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const items = await this.db.scanAll({
                    tableName: this.tableName,
                    ...options,
                });

                let itemsOutput = items as Output<S, C>[];
                if (config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as Output<S, C>[];
                }

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'SCAN',
                        time,
                        duration: hrTimeToMs(start),
                        itemCount: items.length,
                    });
                }

                return itemsOutput;
            }

            async get(keys: GetKeysObj<S, C>, options?: GetOptions): Promise<Output<S, C> | undefined> {
                const time = Date.now();
                const start = process.hrtime();

                const item = await this.db.get({
                    tableName: this.tableName,
                    primaryKey: keys,
                    ...options,
                });

                const output = item !== undefined ? config.transformOutput?.(item) ?? item : undefined;

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'GET',
                        time,
                        duration: hrTimeToMs(start),
                        item,
                    });
                }

                return output;
            }

            async query(key: QueryKeysObj<S, C>, options?: QueryOptions): Promise<Output<S, C>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const items = await this.db.queryAll({
                    tableName: this.tableName,
                    keyConditionExpressionSpec: equal(config.keys[0], key[config.keys[0]]),
                    ...options,
                });

                let itemsOutput = items as Output<S, C>[];
                if (config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as Output<S, C>[];
                }

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'QUERY',
                        time,
                        duration: hrTimeToMs(start),
                        itemCount: items.length,
                    });
                }

                return itemsOutput;
            }

            async create(data: Input<S, C>, options?: CreateOptions): Promise<Output<S, C>> {
                const item = removeUndefined(config.transformInput?.(data) ?? data);
                const conditionExpressionSpec = and(
                    ...config.keys.filter((key): key is string => !!key).map(key => attributeNotExists(key))
                );
                return this.put(item, { ...options, conditionExpressionSpec });
            }

            async put(data: Input<S, C>, options?: PutOptions): Promise<Output<S, C>> {
                const time = Date.now();
                const start = process.hrtime();

                const item = removeUndefined(config.transformInput?.(data) ?? data);

                const prevItem = await this.db.put({
                    tableName: this.tableName,
                    item,
                    returnValues: 'ALL_OLD',
                    ...options,
                });

                const output = config.transformOutput?.(item) ?? item;

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'PUT',
                        time,
                        duration: hrTimeToMs(start),
                        item,
                        prevItem,
                    });
                }

                return output;
            }

            async update(keys: UpdateKeysObj<S, C>, dataOrExpression: UpdateData<S, C>, options?: UpdateOptions): Promise<Output<S, C>> {
                const time = Date.now();
                const start = process.hrtime();

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

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'UPDATE',
                        time,
                        duration: hrTimeToMs(start),
                        item,
                    });
                }

                return output;
            }

            async delete(keys: DeleteKeysObj<S, C>, options?: DeleteOptions): Promise<Output<S, C> | undefined> {
                const time = Date.now();
                const start = process.hrtime();

                const prevItem = await this.db.delete({
                    tableName: this.tableName,
                    returnValues: 'ALL_OLD',
                    primaryKey: keys,
                    ...options,
                });

                const prevOutput = prevItem !== undefined ? config.transformOutput?.(prevItem) ?? prevItem : undefined;

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'DELETE',
                        time,
                        duration: hrTimeToMs(start),
                        prevItem,
                    });
                }

                return prevOutput;
            }

            async scanGsi<G extends GsiNames<S, C>>(gsiName: G, options?: ScanOptions): Promise<GsiOutput<S, C, G>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const items = await this.db.scanAll({
                    tableName: this.tableName,
                    indexName: gsiName as string,
                    ...options,
                });

                let itemsOutput = items as GsiOutput<S, C, G>[];

                if (!config.gsis?.[gsiName as string]?.schema && config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as GsiOutput<S, C, G>[];
                }

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'SCAN',
                        time,
                        duration: hrTimeToMs(start),
                        itemCount: items.length,
                        indexName: gsiName as string,
                    });
                }

                return itemsOutput;
            }

            async queryGsi<G extends GsiNames<S, C>>(
                gsiName: G,
                keys: QueryGsiKeysObj<S, C, G>,
                options?: QueryGsiOptions
            ): Promise<GsiOutput<S, C, G>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const gsi = config.gsis?.[String(gsiName)];
                if (!gsi) {
                    throw new Error(`Unrecognized GSI "${String(gsiName)}"`);
                }

                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const keyConditionExpressionSpec = and(...Object.keys(removeUndefined(keys)).map(k => equal(k, (keys as any)[k])));

                const items = await this.db.queryAll({
                    tableName: this.tableName,
                    indexName: gsiName as string,
                    keyConditionExpressionSpec,
                    ...options,
                });

                let itemsOutput = items as GsiOutput<S, C, G>[];

                if (!config.gsis?.[gsiName as string]?.schema && config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as GsiOutput<S, C, G>[];
                }

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'QUERY',
                        time,
                        duration: hrTimeToMs(start),
                        itemCount: items.length,
                        indexName: gsiName as string,
                    });
                }

                return itemsOutput;
            }

            async batchGet(
                batchKeys: GetKeysObj<S, C>[],
                options?: Omit<Dynamon.BatchGet.Operation, 'primaryKeys'>
            ): Promise<{ responses: Static<S>[]; unprocessed: GetKeysObj<S, C>[] | undefined }> {
                const { responses, unprocessed } = await this.db.batchGet({
                    [this.tableName]: {
                        primaryKeys: batchKeys,
                        ...options,
                    },
                });

                return {
                    responses: (responses[this.tableName] as Static<S>[] | undefined) ?? [],
                    unprocessed: unprocessed?.[this.tableName]?.primaryKeys as GetKeysObj<S, C>[] | undefined,
                };
            }

            async batchWrite(
                batchOps: ({ type: 'Delete'; primaryKey: DeleteKeysObj<S, C> } | { type: 'Put'; item: Input<S, C> })[]
            ): Promise<((DeleteKeysObj<S, C> & { type: 'Delete' }) | { type: 'Put'; item: Input<S, C> })[] | undefined> {
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

                return response[this.tableName] as
                    | ((DeleteKeysObj<S, C> & { type: 'Delete' }) | { type: 'Put'; item: Input<S, C> })[]
                    | undefined;
            }

            async batchDelete(batchKeys: DeleteKeysObj<S, C>[]): Promise<DeleteKeysObj<S, C>[] | undefined> {
                const response = await this.batchWrite(batchKeys.map(primaryKey => ({ type: 'Delete', primaryKey })));
                if (!response || !response.length) return;

                return response.map(op => (op as Dynamon.BatchWrite.Delete).primaryKey) as DeleteKeysObj<S, C>[];
            }

            async batchPut(batchItems: Input<S, C>[]): Promise<Input<S, C>[] | undefined> {
                const response = await this.batchWrite(batchItems.map(item => ({ type: 'Put', item })));
                if (!response || !response.length) return;

                return response.map(op => (op as Dynamon.BatchWrite.Put).item) as Input<S, C>[];
            }
        }

        return DdbRepository;
    };

const JobSchema = Type.Object({ id: Type.String(), name: Type.String(), createdAt: Type.Number() });
type Job = Static<typeof JobSchema>;
export class JobRepository extends makeDdbRepository(JobSchema)({
    keys: ['id'],
    transformInput: (input: DistOmit<Job, 'id' | 'createdAt'>): Job => ({
        id: `J123`,
        createdAt: Date.now(),
        ...input,
    }),
    gsis: {
        byName: {
            schema: JobSchema,
            keys: ['name', 'createdAt'],
        },
    },
}) {}

const repo = new JobRepository();
repo.queryGsi('byName', { name: '123' });
