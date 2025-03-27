import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import type { Static, TSchema } from '@sinclair/typebox';
import { and, attributeNotExists, Dynamon, equal, project, update } from '@typemon/dynamon';
import { ExpressionSpec, isExpressionSpec } from '@typemon/dynamon/dist/expression-spec';
import { EventEmitter } from 'events';
import sift from 'sift';
import TypedEventEmitter from 'typed-emitter';

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
    DdbRepositoryEvents,
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
    ScanGsiOptions,
    ScanOptions,
    UpdateData,
    UpdateKeysObj,
    UpdateOptions,
} from './types.js';
import { buildUpdateExpression, hrTimeToMs, removeUndefined } from './util.js';

// TODO: FIX ME
// "limit" in queryAll command just limits the size of each
// page, and will still scan the entire table...regular "query"
// only returns first page. Solution is to use paginated query
// and concatenateWhile() to break on the scannedCount

export const makeDdbRepository =
    <S extends TSchema>(schema: S) =>
    <C extends DdbRepositoryConfig<S>>(config: C) => {
        abstract class DdbRepository {
            readonly schema: S = schema;
            readonly client: DynamoDBClient;
            readonly db: Dynamon;
            readonly tableName: string;
            readonly validate: boolean;
            readonly logger = new EventEmitter() as TypedEventEmitter<DdbRepositoryEvents<S>>;

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
            }

            // since typescript doesn't always catch "extra" properties on an object
            // type depending on the context, this utility function can be used to
            // make sure all other properties get stripped
            getPrimaryKey(keys: GetKeysObj<S, C>): GetKeysObj<S, C> {
                const primaryKey = { [config.partitionKey]: keys[config.partitionKey] } as GetKeysObj<S, C>;
                if (!config.sortKey) {
                    return primaryKey;
                }
                return { ...primaryKey, [config.sortKey]: keys[config.sortKey] };
            }

            doItemKeysMatch(keys1: GetKeysObj<S, C>, keys2: GetKeysObj<S, C>): boolean {
                if (keys1[config.partitionKey] !== keys2[config.partitionKey]) return false;
                return config.sortKey ? keys1[config.sortKey] !== keys2[config.sortKey] : true;
            }

            async scan(options?: ScanOptions): Promise<Output<S, C>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const itemsOutput: Output<S, C>[] = [];
                for await (const items of this.scanPaged(options)) {
                    itemsOutput.push(...items);
                }

                if (options?.log !== false) {
                    this.logger.emit('operation', {
                        operation: 'SCAN',
                        time,
                        duration: hrTimeToMs(start),
                        itemCount: itemsOutput.length,
                    });
                }

                return itemsOutput;
            }

            async *scanPaged(options?: ScanOptions): AsyncGenerator<Output<S, C>[]> {
                const paginator = this.db.scan$({
                    tableName: this.tableName,
                    skipEmptyPage: true,
                    ...options,
                });

                for await (const page of paginator) {
                    let itemsOutput = page.items as Output<S, C>[];
                    if (config.transformOutput) {
                        itemsOutput = page.items.map(config.transformOutput) as Output<S, C>[];
                    }
                    yield itemsOutput;
                }
            }

            async get(keys: GetKeysObj<S, C>, options?: GetOptions<S, C>): Promise<Output<S, C> | undefined> {
                const time = Date.now();
                const start = process.hrtime();

                const { assert, ...otherOptions } = options ?? {};

                const item = await this.db.get({
                    tableName: this.tableName,
                    primaryKey: this.getPrimaryKey(keys),
                    ...otherOptions,
                });

                let output = item ? (config.transformOutput?.(item) ?? item) : undefined;

                if (output && assert) {
                    const checkAssertion = typeof assert === 'function' ? assert : sift(assert);
                    if (!checkAssertion(item)) {
                        output = undefined;
                    }
                }

                if (options?.log !== false) {
                    this.logger.emit('operation', {
                        operation: 'GET',
                        time,
                        duration: hrTimeToMs(start),
                        item,
                    });
                }

                return output;
            }

            async getOrThrow(keys: GetKeysObj<S, C>, options?: GetOptions<S, C>): Promise<Output<S, C>> {
                const item = await this.get(keys, options);
                if (!item) {
                    throw new Error(`Item in table ${this.tableName} not found.`);
                }

                return item;
            }

            async query(key: QueryKeysObj<S, C>, options?: QueryOptions): Promise<Output<S, C>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const conditions: ExpressionSpec[] = [equal(config.partitionKey, key[config.partitionKey])];

                const { keyConditionExpressionSpec: sortKeyCondition, ...otherOptions } = options ?? {};
                if (sortKeyCondition) {
                    conditions.push(sortKeyCondition);
                }

                const result = await this.db.query({
                    tableName: this.tableName,
                    keyConditionExpressionSpec: and(conditions),
                    concatenateWhile: page => {
                        if (!options?.limit) return true;
                        return page.scannedCount < options.limit;
                    },
                    ...otherOptions,
                });

                let itemsOutput = result.items as Output<S, C>[];
                if (config.transformOutput) {
                    itemsOutput = result.items.map(config.transformOutput) as Output<S, C>[];
                }

                if (options?.log !== false) {
                    this.logger.emit('operation', {
                        operation: 'QUERY',
                        time,
                        duration: hrTimeToMs(start),
                        itemCount: itemsOutput.length,
                    });
                }

                return itemsOutput;
            }

            async *queryPaged(key: QueryKeysObj<S, C>, options?: QueryOptions): AsyncGenerator<Output<S, C>[]> {
                const conditions: ExpressionSpec[] = [equal(config.partitionKey, key[config.partitionKey])];

                const { keyConditionExpressionSpec: sortKeyCondition, ...otherOptions } = options ?? {};
                if (sortKeyCondition) {
                    conditions.push(sortKeyCondition);
                }

                const paginator = this.db.query$({
                    tableName: this.tableName,
                    keyConditionExpressionSpec: and(conditions),
                    skipEmptyPage: true,
                    ...otherOptions,
                });

                for await (const page of paginator) {
                    let itemsOutput = page.items as Output<S, C>[];
                    if (config.transformOutput) {
                        itemsOutput = page.items.map(config.transformOutput) as Output<S, C>[];
                    }
                    yield itemsOutput;
                }
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
                    this.logger.emit('operation', {
                        operation: 'PUT',
                        time,
                        duration: hrTimeToMs(start),
                        item,
                        prevItem,
                    });

                    this.logger.emit('write', {
                        operation: 'PUT',
                        time,
                        item,
                    });
                }

                return output;
            }

            async update(keys: UpdateKeysObj<S, C>, dataOrExpression: UpdateData<S, C>, options?: UpdateOptions): Promise<Output<S, C>> {
                const time = Date.now();
                const start = process.hrtime();

                const updateExpressionSpec = isExpressionSpec(dataOrExpression)
                    ? dataOrExpression
                    : update(buildUpdateExpression(dataOrExpression));

                const item = await this.db.update({
                    tableName: this.tableName,
                    primaryKey: this.getPrimaryKey(keys),
                    returnValues: 'ALL_NEW',
                    updateExpressionSpec,
                    ...options,
                });

                const output = config.transformOutput?.(item) ?? item;

                if (options?.log !== false) {
                    this.logger.emit('operation', {
                        operation: 'UPDATE',
                        time,
                        duration: hrTimeToMs(start),
                        item,
                    });

                    this.logger.emit('write', {
                        operation: 'UPDATE',
                        time,
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

                const prevOutput = prevItem ? (config.transformOutput?.(prevItem) ?? prevItem) : undefined;

                if (options?.log !== false) {
                    this.logger.emit('operation', {
                        operation: 'DELETE',
                        time,
                        duration: hrTimeToMs(start),
                        prevItem,
                    });
                }

                return prevOutput;
            }

            async scanGsi<G extends GsiNames<S, C>>(indexName: G, options?: ScanGsiOptions): Promise<GsiOutput<S, C, G>[]> {
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
                    this.logger.emit('operation', {
                        operation: 'SCAN',
                        time,
                        indexName,
                        duration: hrTimeToMs(start),
                        itemCount: items.length,
                    });
                }

                return itemsOutput;
            }

            async *scanGsiPaged<G extends GsiNames<S, C>>(indexName: G, options?: ScanGsiOptions): AsyncGenerator<GsiOutput<S, C, G>[]> {
                const gsi = config.gsis?.[indexName];
                if (!gsi) {
                    throw new Error(`Unrecognized GSI "${indexName}"`);
                }

                const paginator = this.db.scan$({
                    tableName: this.tableName,
                    skipEmptyPage: true,
                    indexName,
                    ...options,
                });

                for await (const page of paginator) {
                    let itemsOutput = page.items as GsiOutput<S, C, G>[];

                    if ((gsi.projection === 'ALL' || !gsi?.projection) && config.transformOutput) {
                        itemsOutput = page.items.map(config.transformOutput) as GsiOutput<S, C, G>[];
                    }

                    yield itemsOutput;
                }
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

                const result = await this.db.query({
                    tableName: this.tableName,
                    indexName,
                    keyConditionExpressionSpec: and(conditions),
                    concatenateWhile: page => {
                        if (!options?.limit) return true;
                        return page.scannedCount < options.limit;
                    },
                    ...otherOptions,
                });

                let itemsOutput = result.items as GsiOutput<S, C, G>[];

                if ((gsi.projection === 'ALL' || !gsi?.projection) && config.transformOutput) {
                    itemsOutput = result.items.map(config.transformOutput) as GsiOutput<S, C, G>[];
                }

                if (options?.log !== false) {
                    this.logger.emit('operation', {
                        operation: 'QUERY',
                        time,
                        indexName,
                        duration: hrTimeToMs(start),
                        itemCount: itemsOutput.length,
                    });
                }

                return itemsOutput;
            }

            async *queryGsiPaged<G extends GsiNames<S, C>>(
                indexName: G,
                keys: QueryGsiKeysObj<S, C, G>,
                options?: QueryGsiOptions
            ): AsyncGenerator<GsiOutput<S, C, G>[]> {
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

                const paginator = this.db.query$({
                    tableName: this.tableName,
                    indexName,
                    keyConditionExpressionSpec: and(conditions),
                    skipEmptyPage: true,
                    ...otherOptions,
                });

                for await (const page of paginator) {
                    let itemsOutput = page.items as GsiOutput<S, C, G>[];
                    if (config.transformOutput) {
                        if ((gsi.projection === 'ALL' || !gsi?.projection) && config.transformOutput) {
                            itemsOutput = page.items.map(config.transformOutput) as GsiOutput<S, C, G>[];
                        }
                    }
                    yield itemsOutput;
                }
            }

            async batchGet(batchKeys: GetKeysObj<S, C>[], options?: BatchGetOptions<S, C>): Promise<BatchGetOutput<S, C>> {
                const { assert, maxParallelRequests = 1, ...otherOptions } = options ?? {};

                const time = Date.now();
                const start = process.hrtime();

                // remove duplicate keys
                const primaryKeysMap = new Map<string, GetKeysObj<S, C>>();
                batchKeys.map(keys => {
                    const pks = this.getPrimaryKey(keys);
                    primaryKeysMap.set(JSON.stringify(pks), pks);
                });

                const parallelBatchCount = Math.min(Math.ceil(primaryKeysMap.size / 100), maxParallelRequests);

                let i = 0;
                const parallelBatches = Array.from({ length: 6 }).map<GetKeysObj<S, C>[]>(() => []);
                for (const pk of primaryKeysMap.values()) {
                    parallelBatches[i++ % parallelBatchCount].push(pk);
                }

                const items: Static<S>[] = [];
                const unprocessed: GetKeysObj<S, C>[] = [];

                await Promise.all(
                    parallelBatches.map(async primaryKeys => {
                        return this.db
                            .batchGet({
                                [this.tableName]: { primaryKeys, ...otherOptions },
                            })
                            .then(result => {
                                items.push(...result.responses[this.tableName]);
                                unprocessed.push(
                                    ...((result.unprocessed?.[this.tableName].primaryKeys as GetKeysObj<S, C>[] | undefined) ?? [])
                                );
                            });
                    })
                );

                let itemsOutput = items as Output<S, C>[];
                if (config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as Output<S, C>[];
                }

                if (assert) {
                    const checkAssertion = typeof assert === 'function' ? assert : sift(assert);
                    itemsOutput = itemsOutput.filter(item => checkAssertion(item));
                }

                if (options?.log !== false) {
                    this.logger.emit('operation', {
                        operation: 'BATCH_GET',
                        time,
                        duration: hrTimeToMs(start),
                        unprocessedCount: unprocessed.length ?? 0,
                    });
                }

                return {
                    items: itemsOutput,
                    unprocessed,
                };
            }

            // TODO: should normalize response to use "keys" intead of primaryKey
            // so the unprocessed can be fed back in
            // TODO: retry unprocessed items w/ exponential backoff
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
                    this.logger.emit('operation', {
                        operation: 'BATCH_WRITE',
                        time,
                        duration: hrTimeToMs(start),
                        unprocessedCount,
                    });
                }

                if (!unprocessedCount || !response) return [];

                return response[this.tableName].map(r => {
                    if (r.type !== 'Put') return r;

                    if (options?.log !== false) {
                        this.logger.emit('write', {
                            operation: 'BATCH_WRITE',
                            time,
                            item: r.item,
                        });
                    }

                    return {
                        ...r,
                        item: config.transformOutput?.(r.item) ?? r.item,
                    };
                }) as BatchWriteOutput<S, C>;
            }

            async batchDelete(batchKeys: DeleteKeysObj<S, C>[], options?: BatchDeleteOptions): Promise<DeleteKeysObj<S, C>[]> {
                const response = await this.batchWrite(
                    batchKeys.map(keys => ({ type: 'Delete', keys })),
                    options
                );
                if (!response || !response.length) return [];

                return response.map(op => (op as Dynamon.BatchWrite.Delete).primaryKey) as DeleteKeysObj<S, C>[];
            }

            async batchPut(batchItems: Input<S, C>[], options?: BatchPutOptions): Promise<Output<S, C>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const putOps = batchItems.map(item => ({
                    type: 'Put' as const,
                    item: removeUndefined(config.transformInput?.(item) ?? item),
                }));

                const response = await this.db.batchWrite({ [this.tableName]: putOps });

                const unprocessedItems = (response?.[this.tableName] ?? []).flatMap(op =>
                    op.type === 'Put' ? (op.item as Input<S, C>) : []
                );

                // if we have unprocessed items, delete any that were successfully procesed before throwing
                if (unprocessedItems.length) {
                    const processedItems = batchItems.filter(item => {
                        const itemPk = this.getPrimaryKey(item);
                        return !unprocessedItems.some(unprocessedItem => this.doItemKeysMatch(unprocessedItem, itemPk));
                    });

                    await this.batchDelete(processedItems);

                    throw new Error(
                        `Batch PUT failed to process ${unprocessedItems.length} item(s). Successfully processed items have been deleted.`
                    );
                }

                const outputItems = putOps.map(op => {
                    if (options?.log !== false) {
                        this.logger.emit('write', {
                            operation: 'BATCH_WRITE',
                            time,
                            item: op.item,
                        });
                    }
                    return config.transformOutput?.(op.item) ?? op.item;
                });

                if (options?.log !== false) {
                    this.logger.emit('operation', {
                        operation: 'BATCH_WRITE',
                        time,
                        duration: hrTimeToMs(start),
                        unprocessedCount: 0,
                    });
                }

                return outputItems;
            }

            async exists(
                options?: Omit<QueryOptions, 'projectionExpressionSpec' | 'limit'> & { keys?: QueryKeysObj<S, C> }
            ): Promise<boolean> {
                const { keys, filterExpressionSpec, ...optionsWithoutKeys } = options ?? {};

                // we can't rely on the limit when a filter expression spec is being passed, since the filter is
                // applied after the scan/query page, so we only apply a limit of 1 when there is no filterExpressionSpec.
                // otherwise, we page through the results and return true as soon as we see a single item
                const optionsWithLimit: QueryOptions = {
                    ...optionsWithoutKeys,
                    filterExpressionSpec,
                    projectionExpressionSpec: project(config.partitionKey),
                    limit: filterExpressionSpec ? undefined : 1,
                };

                const pagedItems = keys ? this.queryPaged(keys, optionsWithLimit) : this.scanPaged(optionsWithLimit);

                for await (const items of pagedItems) {
                    if (items.length) return true;
                }

                return false;
            }

            async existsGsi<G extends GsiNames<S, C>>(
                gsi: G,
                options?: Omit<QueryGsiOptions, 'projectionExpressionSpec' | 'limit'> & { keys?: QueryGsiKeysObj<S, C, G> }
            ): Promise<boolean> {
                const { keys, filterExpressionSpec, ...optionsWithoutKeys } = options ?? {};

                // we can't rely on the limit when a filter expression spec is being passed, since the filter is
                // applied after the scan/query page, so we only apply a limit of 1 when there is no filterExpressionSpec.
                // otherwise, we page through the results and return true as soon as we see a single item
                const optionsWithLimit: QueryOptions = {
                    ...optionsWithoutKeys,
                    filterExpressionSpec,
                    projectionExpressionSpec: project(config.partitionKey),
                    limit: filterExpressionSpec ? undefined : 1,
                };

                const pagedItems = keys ? this.queryGsiPaged(gsi, keys, optionsWithLimit) : this.scanGsiPaged(gsi, optionsWithLimit);

                for await (const items of pagedItems) {
                    if (items.length) return true;
                }

                return false;
            }
        }

        return DdbRepository;
    };
