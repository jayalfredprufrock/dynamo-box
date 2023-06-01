import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { Static, TSchema } from '@sinclair/typebox';
import { and, attributeNotExists, Dynamon, equal, set, update } from '@typemon/dynamon';
import { isExpressionSpec } from '@typemon/dynamon/dist/expression-spec';

import {
    CreateOptions,
    DdbRepositoryConfig,
    DdbRepositoryLogger,
    DdbRepositoryRuntimeConfig,
    DeleteOptions,
    GetOptions,
    GsiKeys,
    GsiKeysObj,
    Input,
    KeysObj,
    Output,
    PrimaryKeyObj,
    PutOptions,
    QueryOptions,
    ScanOptions,
    UpdateData,
    UpdateOptions,
} from './types.js';
import { hrTimeToMs, removeUndefined } from './util.js';

export const makeDdbRepository =
    <Schema extends TSchema>(schema: Schema) =>
    <C extends DdbRepositoryConfig<Schema>>(config: C) => {
        abstract class DdbRepository {
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

            async get(keys: KeysObj<Schema, C>, options?: GetOptions): Promise<Output<Schema, C> | undefined> {
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

            async scan(options?: ScanOptions): Promise<Output<Schema, C>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const items = await this.db.scanAll({
                    tableName: this.tableName,
                    ...options,
                });

                let itemsOutput = items as Output<Schema, C>[];
                if (config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as Output<Schema, C>[];
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

            async query(key: PrimaryKeyObj<Schema, C>, options?: QueryOptions): Promise<Output<Schema, C>[]> {
                const time = Date.now();
                const start = process.hrtime();

                const items = await this.db.queryAll({
                    tableName: this.tableName,
                    keyConditionExpressionSpec: equal(config.keys[0], key[config.keys[0]]),
                    ...options,
                });

                let itemsOutput = items as Output<Schema, C>[];
                if (config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as Output<Schema, C>[];
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

            async queryGsi<G extends GsiKeys<Schema, C>>(
                name: G,
                keys: GsiKeysObj<Schema, C, G>,
                options?: QueryOptions
            ): Promise<Output<Schema, C>[]> {
                const time = Date.now();
                const start = process.hrtime();

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

                let itemsOutput = items as Output<Schema, C>[];
                if (config.transformOutput) {
                    itemsOutput = items.map(config.transformOutput) as Output<Schema, C>[];
                }

                if (options?.log !== false) {
                    this.logger?.({
                        operation: 'QUERY',
                        time,
                        duration: hrTimeToMs(start),
                        itemCount: items.length,
                        gsi: name as string,
                    });
                }

                return itemsOutput;
            }

            async create(data: Input<Schema, C>, options?: CreateOptions): Promise<Output<Schema, C>> {
                const item = removeUndefined(config.transformInput?.(data) ?? data);
                const conditionExpressionSpec = and(
                    ...config.keys.filter((key): key is string => !!key).map(key => attributeNotExists(key))
                );
                return this.put(item, { ...options, conditionExpressionSpec });
            }

            async put(data: Input<Schema, C>, options?: PutOptions): Promise<Output<Schema, C>> {
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

            async update(
                keys: KeysObj<Schema, C>,
                dataOrExpression: UpdateData<Schema, C>,
                options?: UpdateOptions
            ): Promise<Output<Schema, C>> {
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

            async delete(keys: KeysObj<Schema, C>, options?: DeleteOptions): Promise<Output<Schema, C> | undefined> {
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

            async batchGet(
                batchKeys: KeysObj<Schema, C>[],
                options?: Omit<Dynamon.BatchGet.Operation, 'primaryKeys'>
            ): Promise<{ responses: Static<Schema>[]; unprocessed: KeysObj<Schema, C>[] | undefined }> {
                const { responses, unprocessed } = await this.db.batchGet({
                    [this.tableName]: {
                        primaryKeys: batchKeys,
                        ...options,
                    },
                });

                return {
                    responses: (responses[this.tableName] as Static<Schema>[] | undefined) ?? [],
                    unprocessed: unprocessed?.[this.tableName]?.primaryKeys as KeysObj<Schema, C>[] | undefined,
                };
            }

            async batchWrite(
                batchOps: ({ type: 'Delete'; primaryKey: KeysObj<Schema, C> } | { type: 'Put'; item: Input<Schema, C> })[]
            ): Promise<((KeysObj<Schema, C> & { type: 'Delete' }) | { type: 'Put'; item: Input<Schema, C> })[] | undefined> {
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
                    | ((KeysObj<Schema, C> & { type: 'Delete' }) | { type: 'Put'; item: Input<Schema, C> })[]
                    | undefined;
            }

            async batchDelete(batchKeys: KeysObj<Schema, C>[]): Promise<KeysObj<Schema, C>[] | undefined> {
                const response = await this.batchWrite(batchKeys.map(primaryKey => ({ type: 'Delete', primaryKey })));
                if (!response || !response.length) return;

                return response.map(op => (op as Dynamon.BatchWrite.Delete).primaryKey) as KeysObj<Schema, C>[];
            }

            async batchPut(batchItems: Input<Schema, C>[]): Promise<Input<Schema, C>[] | undefined> {
                const response = await this.batchWrite(batchItems.map(item => ({ type: 'Put', item })));
                if (!response || !response.length) return;

                return response.map(op => (op as Dynamon.BatchWrite.Put).item) as Input<Schema, C>[];
            }
        }

        return DdbRepository;
    };

/*
const JobSchema = Type.Object({ id: Type.String(), name: Type.String() });
type Job = Static<typeof JobSchema>;
export class JobRepository extends makeDdbRepository(JobSchema)({
    keys: ['id'],
    transformInput: (input: DistOmit<Job, 'id'>): Job => ({
        id: `J123`,
        ...input,
    }),
    transformOutput: (output: Job) => {
        return {
            ...output,
            now: Date.now(),
        };
    },
    gsis: {
        byNAme: {
            schema: JobSchema,
            keys: ['name'],
        },
    },
}) {}
*/
