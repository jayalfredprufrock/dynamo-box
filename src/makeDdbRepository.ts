import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Static, TSchema } from '@sinclair/typebox';
import { and, attributeNotExists, Dynamon, equal, set, update } from '@typemon/dynamon';
import { ExpressionSpec, isExpressionSpec } from '@typemon/dynamon/dist/expression-spec';

import {
    BatchDeleteOptions,
    BatchGetOptions,
    BatchGetOutput,
    BatchPutOptions,
    BatchWriteOps,
    BatchWriteOptions,
    BatchWriteOutput,
    CreateOptions,
    DdbRepositoryConfig,
    DdbRepositoryLogger,
    DdbRepositoryRuntimeConfig,
    DeleteKeysObj,
    DeleteOptions,
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

            // since typescript doesn't always catch "extra" properties on an object
            // type depending on the context, this utility function can be used to
            // make sure all other properties get stripped
            getPrimaryKey(keys: GetKeysObj<S, C>): GetKeysObj<S, C> {
                if (!config.sortKey) {
                    return { [config.partitionKey]: keys[config.partitionKey] } as GetKeysObj<S, C>;
                }
                return { [config.partitionKey]: keys[config.partitionKey], [config.sortKey]: keys[config.sortKey] } as GetKeysObj<S, C>;
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
                    primaryKey: this.getPrimaryKey(keys),
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

                const conditions: ExpressionSpec[] = [equal(config.partitionKey, key[config.partitionKey])];

                const { keyConditionExpressionSpec: rangeCondition, ...otherOptions } = options ?? {};
                if (rangeCondition) {
                    conditions.push(rangeCondition);
                }

                const items = await this.db.queryAll({
                    tableName: this.tableName,
                    keyConditionExpressionSpec: and(conditions),
                    ...otherOptions,
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
                const conditionExpressionSpec = attributeNotExists(config.partitionKey);
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
                    primaryKey: this.getPrimaryKey(keys),
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
                    primaryKey: this.getPrimaryKey(keys),
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

            async scanGsi<G extends GsiNames<S, C>>(indexName: G, options?: ScanOptions): Promise<GsiOutput<S, C, G>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const gsi = config.gsis?.[indexName];
                if (!gsi) {
                    throw new Error(`Unrecognized GSI "${indexName}"`);
                }

                const items = await this.db.scanAll({
                    tableName: this.tableName,
                    indexName,
                    ...options,
                });

                let itemsOutput = items as GsiOutput<S, C, G>[];

                if ((gsi.projection === 'ALL' || !gsi?.projection) && config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as GsiOutput<S, C, G>[];
                }

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'SCAN',
                        time,
                        indexName,
                        duration: hrTimeToMs(start),
                        itemCount: items.length,
                    });
                }

                return itemsOutput;
            }

            async queryGsi<G extends GsiNames<S, C>>(
                indexName: G,
                keys: QueryGsiKeysObj<S, C, G>,
                options?: QueryGsiOptions
            ): Promise<GsiOutput<S, C, G>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const gsi = config.gsis?.[indexName];
                if (!gsi) {
                    throw new Error(`Unrecognized GSI "${indexName}"`);
                }

                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const conditions: ExpressionSpec[] = Object.keys(removeUndefined(keys)).map(k => equal(k, (keys as any)[k]));
                const { keyConditionExpressionSpec: rangeCondition, ...otherOptions } = options ?? {};
                if (rangeCondition) {
                    conditions.push(rangeCondition);
                }

                const items = await this.db.queryAll({
                    tableName: this.tableName,
                    indexName,
                    keyConditionExpressionSpec: and(conditions),
                    ...otherOptions,
                });

                let itemsOutput = items as GsiOutput<S, C, G>[];

                if ((gsi.projection === 'ALL' || !gsi?.projection) && config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as GsiOutput<S, C, G>[];
                }

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'QUERY',
                        time,
                        indexName,
                        duration: hrTimeToMs(start),
                        itemCount: items.length,
                    });
                }

                return itemsOutput;
            }

            async batchGet(batchKeys: GetKeysObj<S, C>[], options?: BatchGetOptions): Promise<BatchGetOutput<S, C>> {
                const time = Date.now();
                const start = process.hrtime();

                const { responses, unprocessed } = await this.db.batchGet({
                    [this.tableName]: {
                        primaryKeys: batchKeys.map(this.getPrimaryKey),
                        ...options,
                    },
                });

                const items = (responses[this.tableName] as Static<S>[] | undefined) ?? [];
                let itemsOutput = items as Output<S, C>[];
                if (config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as Output<S, C>[];
                }

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'BATCH_GET',
                        time,
                        duration: hrTimeToMs(start),
                        unprocessedCount: unprocessed?.[this.tableName]?.primaryKeys.length ?? 0,
                    });
                }

                return {
                    items: itemsOutput,
                    unprocessed: unprocessed?.[this.tableName]?.primaryKeys as GetKeysObj<S, C>[] | undefined,
                };
            }

            async batchWrite(batchOps: BatchWriteOps<S, C>, options?: BatchWriteOptions): Promise<BatchWriteOutput<S, C>> {
                const time = Date.now();
                const start = process.hrtime();

                const response = await this.db.batchWrite({
                    [this.tableName]: batchOps.map(op => {
                        if (op.type === 'Put') {
                            return {
                                type: 'Put',
                                item: removeUndefined(config.transformInput?.(op.item) ?? op.item),
                            };
                        }
                        return {
                            type: 'Delete',
                            primaryKey: this.getPrimaryKey(op.keys),
                        };
                    }),
                });

                const unprocessedCount = response?.[this.tableName]?.length ?? 0;

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'BATCH_WRITE',
                        time,
                        duration: hrTimeToMs(start),
                        unprocessedCount,
                    });
                }

                if (!unprocessedCount || !response) return;

                return response[this.tableName] as BatchWriteOutput<S, C>;
            }

            async batchDelete(batchKeys: DeleteKeysObj<S, C>[], options?: BatchDeleteOptions): Promise<DeleteKeysObj<S, C>[] | undefined> {
                const response = await this.batchWrite(
                    batchKeys.map(keys => ({ type: 'Delete', keys })),
                    options
                );
                if (!response || !response.length) return;

                return response.map(op => (op as Dynamon.BatchWrite.Delete).primaryKey) as DeleteKeysObj<S, C>[];
            }

            async batchPut(batchItems: Input<S, C>[], options?: BatchPutOptions): Promise<Input<S, C>[] | undefined> {
                const response = await this.batchWrite(
                    batchItems.map(item => ({ type: 'Put', item })),
                    options
                );
                if (!response || !response.length) return;

                return response.map(op => (op as Dynamon.BatchWrite.Put).item) as Input<S, C>[];
            }
        }

        return DdbRepository;
    };

/*
const JobSchema = Type.Union([
    Type.Object({
        id: Type.String(),
        type: Type.Literal('RECURRING'),
        name: Type.String(),
        recurringId: Type.String(),
        description: Type.Optional(Type.String()),
        createdAt: Type.Number(),
        updatedAt: Type.Number(),
    }),
    Type.Object({
        id: Type.String(),
        type: Type.Literal('ONESHOT'),
        name: Type.String(),
        description: Type.Optional(Type.String()),
        createdAt: Type.Number(),
        updatedAt: Type.Number(),
    }),
]);

type Job = Static<typeof JobSchema>;
export class JobRepository extends makeDdbRepository(JobSchema)({
    partitionKey: 'id',
    sortKey: 'createdAt',
    transformInput: (input: DistOmit<Job, 'id' | 'updatedAt' | 'createdAt'>): Job => ({
        id: `J123`,
        createdAt: Date.now(),
        ...input,
        updatedAt: Date.now(),
    }),
    transformOutput: (output: Job) => ({
        ...output,
        extraField: 3,
    }),
    gsis: {
        byName: {
            partitionKey: 'name',
            sortKey: 'updatedAt',
            projection: ['description'],
        },
    },
}) {}

const repo = new JobRepository();
const test = await repo.put({ id: '123', type: 'RECURRING', name: 'asdf', recurringId: '123' });
*/
