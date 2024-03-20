/* eslint-disable @typescript-eslint/no-explicit-any */

import { default as sqlite_introspection_query } from '../introspection/sqlite_introspection_query.sql'
import { default as postgresql_introspection_query } from '../introspection/postgresql_introspection_query.sql'
import { default as clickhouse_introspection_query } from '../introspection/clickhouse_introspection_query.sql'
import { default as mysql_introspection_query } from '../introspection/mysql_introspection_query.sql'
import type { Pool as PgPool } from 'pg'
import type { Database as WasmSqlite3Database, SqlValue } from '@sqlite.org/sqlite-wasm'
import type { Database as BetterSqliteDatabase } from 'better-sqlite3'
import type { Database as Sqlite3Database } from 'sqlite'
import type { Client as TursoDatabase } from '@libsql/client'
import type { Request as ExpressRequest, NextFunction, Response } from 'express'
import type { IncomingMessage } from 'http'
import type { Express} from 'express'
//type HttpRequest = Request | ExpressRequest | IncomingMessage
// interface SubzeroHttpRequest extends HttpRequest {
//   parsedUrl?: URL,
//   textBody?: string,
//   body?: unknown,
//   headersSequence?: unknown,
//   text?: unknown
// }
//import { default as wasmbin } from '../../subzero-wasm/pkg/subzero_wasm_bg.wasm'
//import /*init, */{ initSync, Backend } from '../../subzero-wasm/pkg/subzero_wasm.js'
//import init, { Backend } from '../../subzero-wasm/pkg/subzero_wasm.js'
//import init, { Backend } from '../../subzero-wasm/pkg/subzero_wasm.js'
//import wasmbin from '../../subzero-wasm/pkg/subzero_wasm_bg.wasm'
//const wasmPromise = init('file:./subzero_wasm_bg.wasm')
//const wasmPromise = init(wasmbin)

// interface RequestWithUser extends Request {
//   user?: any; // Replace 'any' with the actual type of your user object
// }

export type DbType = 'postgresql' | 'sqlite' | 'clickhouse' | 'mysql'
export type PgDatabase = PgPool
export type SqliteDatabase = Sqlite3Database | BetterSqliteDatabase | TursoDatabase | WasmSqlite3Database
export type DbPool = PgDatabase | SqliteDatabase
export type Query = string
export type Parameters = (string | number | boolean | null | (string | number | boolean | null)[])[]
export type Statement = { query: Query, parameters: Parameters }

export type GetParameters = [string, string][]
export type Method = 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
export type Body = string | undefined
export type Headers = [string, string][]
export type Cookies = [string, string][]
export type Env = [string, string][]

export type SchemaColumn = {
    name: string;
    data_type: string;
    primary_key: boolean;
};
export type SchemaForeignKey = {
    name: string;
    table: [string, string];
    columns: string[];
    referenced_table: [string, string];
    referenced_columns: string[];
};
export type SchemaObject = {
    name: string;
    kind: string;
    columns: SchemaColumn[];
    foreign_keys: SchemaForeignKey[];
    permissions: any[]
};
export type Schema = {
    [key: string]: SchemaObject;
};

export class SubzeroError extends Error {
    message: string
    status: number
    description: string | null
    constructor(msg: string, status = 500, description?: string) {
        super(msg)
        this.message = msg
        this.status = status
        this.description = description || null
        // Set the prototype explicitly.
        Object.setPrototypeOf(this, SubzeroError.prototype)
    }
    statusCode(): number {
        return this.status
    }
    toJSONString(): string {
        const { message, description } = this
        return JSON.stringify({ message, description })
    }
}

export interface PgError extends Error {
    severity: string;
    code: string;
    detail?: string;
    hint?: string;
    position?: string;
    internalPosition?: string;
    internalQuery?: string;
    where?: string;
    schema?: string;
    table?: string;
    column?: string;
    dataType?: string;
    constraint?: string;
    file?: string;
    line?: string;
    routine?: string;
}

function isPgError(error: Error): error is PgError {
    return (error as PgError).severity !== undefined;
}

interface SQLiteError extends Error {
    code: string;
}

function isSQLiteError(error: Error): error is SQLiteError {
    return typeof (error as SQLiteError).code === 'string' && (error as SQLiteError).code.startsWith('SQLITE_');
}

// export function isPgPool(pool: DbPool): pool is PgPool {
//     return (pool as PgPool).query !== undefined;
// }

export async function initInternal(
    wasmBackend: any,
    app: Express,
    dbType: DbType,
    dbPool: DbPool,
    dbSchemas: string[],
    options: InitOptions = {},
    wasmInit?: Promise<any>,
): Promise<SubzeroInternal | undefined> {

    let subzero: SubzeroInternal| undefined = undefined;
    const o = {
        useInternalPermissionsCheck: true,
        permissions: [],
        customRelations: [],
        dbMaxConnectionRetries: 10,
        dbMaxConnectionRetryInterval: 10,
        schemaInstanceName: '__schema__',
        subzeroInstanceName: '__subzero__',
        dbPoolInstanceName: '__dbPool__',
        contextEnvInstanceName: '__contextEnv__',
        includeAllDbRoles: false,
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        debugFn: () => {},
        ...options,
    };
    const { query, parameters } = getIntrospectionQuery(
        dbType, // database type
        dbSchemas, // the schema name that is exposed to the HTTP api (ex: public, api)
        // the introspection query has two 'placeholders' in order to be able adapt to different configurations
        new Map([
            ['relations.json', o.customRelations || []],
            ['permissions.json', o.permissions || []],
        ]),
        o.includeAllDbRoles
    );
    o.debugFn("introspection query:\n", query, parameters);
    let wait = 0.5;
    let retries = 0;
    while (!subzero) {
        try {
            // the result of the introspection query is a json string representation of the database schema/structure
            // this schema object is used to generate the queries and check the permissions
            let schema: any;
            if (dbType === 'postgresql') {
                const result = await (dbPool as PgDatabase).query(query, parameters)
                schema = JSON.parse(result.rows[0].json_schema)
            } else if (dbType === 'sqlite' && (isSqlite3Database(dbPool) || isBetterSqliteDatabase(dbPool))) {
                const result: any = await (await dbPool.prepare(query)).get();
                schema = JSON.parse(result.json_schema);
            } else if (dbType === 'sqlite' && isTursoDatabase(dbPool)) {
                const result = await dbPool.execute(query);
                schema = JSON.parse(result.rows[0].json_schema as string);
            } else if (dbType === 'sqlite' && isWasmSqlite3Database(dbPool)) {
                const rows:SqlValue[] = [];
                dbPool.exec({
                    sql: query,
                    resultRows: rows,
                    rowMode: "object",
                });
                schema = JSON.parse((rows as any)[0].json_schema as string);
            
            } else {
                throw new Error(`Database type ${dbType} is not supported`)
            }
            schema.use_internal_permissions = o.useInternalPermissionsCheck;
            const json = JSON.stringify(schema, null, 2);
            const withLineNumbers = json.split('\n').map((line, index) => {
                return `${(index + 1).toString().padStart(4, ' ')}: ${line}`;
            }).join('\n');
            o.debugFn("schema:\n", withLineNumbers);
            subzero = new SubzeroInternal(wasmBackend, dbType, schema, o.allowedSelectFunctions, wasmInit, o.licenseKey);
            await subzero.init(); // not strictly needed in node context
            o.debugFn('Subzero initialized');
            app.set(o.schemaInstanceName, schema);
            o.debugFn('Database schema loaded');
        } catch (e) {
            retries++;
            if (o.dbMaxConnectionRetries > 0 && retries > o.dbMaxConnectionRetries) {
                throw e;
            }
            // check if this is actually a json parse error by look for "invalid json schema" in the error message
            if (e instanceof Error && e.message.indexOf('invalid json schema') !== -1) {
                throw e;
            }

            wait = Math.min(o.dbMaxConnectionRetryInterval, wait * 2);
            console.error(`Failed to connect to database, retrying in ${wait} seconds... (${e})`);
            await new Promise((resolve) => setTimeout(resolve, wait * 1000));
        }
    }

    //app.use(onSubzeroError);
    app.set(o.subzeroInstanceName, subzero);
    app.set(o.dbPoolInstanceName, dbPool);
    let clientType;
    switch (dbType) {
        case 'postgresql': clientType = 'pg'; break;
        case 'sqlite': clientType =
            isBetterSqliteDatabase(dbPool) ? 'better-sqlite3' :
            isSqlite3Database(dbPool) ? 'sqlite3' :
            isTursoDatabase(dbPool) ? 'turso' :
            isWasmSqlite3Database(dbPool) ? 'wasm-sqlite3' : 'unknown';
            break;
        default: clientType = 'unknown';
    }
    if(!clientType || clientType === 'unknown') throw new Error(`DbPool instance is not supported for ${dbType} database type`);
    app.set(`${o.dbPoolInstanceName}_client_type`, clientType);
    if (dbType === 'sqlite' ) {
        const contextEnv = new ContextEnv();
        const boundGetEnvVar = contextEnv.getEnvVar.bind(contextEnv);
        const boundJwt = contextEnv.jwt.bind(contextEnv);
        // if (isBetterSqliteDatabase(dbPool)) {
        //     dbPool.function('env', boundGetEnvVar as (...params: unknown[]) => unknown);
        //     dbPool.function('jwt', boundJwt as () => unknown);
        // }
        switch (clientType) {
            case 'better-sqlite3':
                (dbPool as BetterSqliteDatabase).function('env', boundGetEnvVar as (...params: unknown[]) => unknown);
                (dbPool as BetterSqliteDatabase).function('jwt', boundJwt as () => unknown);
                break;
            case 'wasm-sqlite3':
                (dbPool as WasmSqlite3Database).createFunction('env', function (_n, v) {
                    return boundGetEnvVar(v as string);
                }, { arity: 1, deterministic: true });
                (dbPool as WasmSqlite3Database).createFunction('jwt', function (_n) {
                    return boundJwt();
                }, { arity: 0, deterministic: true });
                break;
        }
        app.set(o.contextEnvInstanceName, contextEnv);
    }
    return subzero;
}
function isSqlite3Database(pool: DbPool): pool is Sqlite3Database {
    return (pool as Sqlite3Database).run !== undefined;
}
function isBetterSqliteDatabase(pool: DbPool): pool is BetterSqliteDatabase {
    return (pool as BetterSqliteDatabase).backup !== undefined;
}

function isTursoDatabase(pool: DbPool): pool is TursoDatabase {
    return (pool as TursoDatabase).sync !== undefined;
}

function isWasmSqlite3Database(pool: DbPool): pool is WasmSqlite3Database {
    return (pool as WasmSqlite3Database).dbVfsName !== undefined;
}

export type InitOptions = {
    useInternalPermissionsCheck?: boolean,
    permissions?: any[],
    customRelations?: any[],
    allowedSelectFunctions?: string[],
    dbMaxConnectionRetries?: number,
    dbMaxConnectionRetryInterval?: number,
    schemaInstanceName?: string,
    subzeroInstanceName?: string,
    dbPoolInstanceName?: string,
    includeAllDbRoles?: boolean,
    debugFn?: (...args: any[]) => void,
    licenseKey?: string,
};

export type HandlerOptions = {
    wrapInTransaction?: boolean,
    setDbEnv?: boolean,
    subzeroInstanceName?: string,
    dbPoolInstanceName?: string,
    contextEnvInstanceName?: string,
    dbAnonRole?: string,
    dbExtraSearchPath?: string[],
    dbMaxRows?: number,
    debugFn?: (...args: any[]) => void,
}

function cleanupRequest(req: ExpressRequest) {
    // delete header prefer if it's empty
    if (req.get('prefer') === '') {
        delete req.headers['prefer'];
    }
}

export class ContextEnv {
    private env: { [key: string]: string };

    constructor() {
        this.env = {};
    }

    setEnv(envMap: { [key: string]: string }): void {
        this.env = envMap;
    }

    getEnvVar(key: string): string | undefined {
        return this.env[key];
    }

    jwt(): string | undefined {
        return this.env['request.jwt.claims'];
    }
}

type DbResponseRow = {
    response_status?: number,
    body?: string,
    page_total?: number,
    total_result_set?: number,
    constraints_satisfied?: boolean,
    response_headers?: string,
}

async function restPg(dbPool: PgDatabase, subzero: SubzeroInternal,
    req: ExpressRequest,
    schema: string,
    prefix: string,
    user: any,
    queryEnv: Env,
    o: HandlerOptions): Promise<DbResponseRow> {

    let transactionStarted = false;
    const db = await dbPool.connect();
    try {
        // generate the SQL query from request object
        const method = req.method || 'GET';
        const { query, parameters } = await subzero.fmtStatement(
            schema,
            prefix,
            user.role,
            req,
            queryEnv,
            o.dbMaxRows,
        );

        const txMode = method === 'GET' ? 'READ ONLY' : 'READ WRITE';
        if (o.wrapInTransaction) {
            await db.query(`BEGIN ISOLATION LEVEL READ COMMITTED ${txMode}`);

            transactionStarted = true;
        }

        if (o.setDbEnv && o.wrapInTransaction) {
            // generate the SQL query that sets the env variables for the current request
            const { query: envQuery, parameters: envParameters } = fmtPostgreSqlEnv(queryEnv);
            o.debugFn && o.debugFn('env query', envQuery, envParameters);
            await db.query(envQuery, envParameters);
        }
        o.debugFn && o.debugFn('main query', query, parameters);
        const result = (await db.query(query, parameters)).rows[0];
        if (o.wrapInTransaction) {
            await db.query('COMMIT');
        }
        db.release();
        return result;

    } catch (e) {
        if (o.wrapInTransaction && transactionStarted) {
            await db.query('ROLLBACK');
        }
        db.release();
        throw e;
    }
}

async function restSqlite(dbPool: SqliteDatabase, subzero: SubzeroInternal,
    req: ExpressRequest,
    schema: string,
    prefix: string,
    user: any,
    queryEnv: Env,
    o: HandlerOptions): Promise<DbResponseRow> {

    const clientType: string = req.app.get(`${o.dbPoolInstanceName}_client_type`);
    const contextEnv: ContextEnv | undefined = req.app.get(o.contextEnvInstanceName as string);
    if (!contextEnv) {
        throw new SubzeroError('Context Env for sqlite not set', 500);
    }
    
    const db = dbPool;
    
    // generate the SQL query from request object
    const method = req.method || 'GET';

    let result = {} as DbResponseRow;
    contextEnv.setEnv(Object.fromEntries(queryEnv));
    if (method == 'GET') {
        const statement = await subzero.fmtStatement(
            schema,
            prefix,
            user.role,
            req,
            queryEnv,
            o.dbMaxRows
        );
        const { query, parameters } = statement as Statement;
        o.debugFn && o.debugFn('env', Object.fromEntries(queryEnv));
        o.debugFn && o.debugFn('main query', query, parameters);
        
        switch (clientType) {
            case 'better-sqlite3':
                result = (db as BetterSqliteDatabase).prepare(query).get(parameters) as DbResponseRow;
                break;
            case 'sqlite3':
                result = await(await (db as Sqlite3Database).prepare(query)).get(parameters) as DbResponseRow;
                break;
            case 'turso':
                result = (await (db as TursoDatabase).execute({
                    sql: query,
                    args: parameters as any,
                })).rows[0] as DbResponseRow;
                break;
            case 'wasm-sqlite3': {
                const rows:SqlValue[] = [];
                (db as WasmSqlite3Database).exec({
                    sql: query,
                    bind: parameters as any,
                    resultRows: rows,
                    rowMode: "object",
                });
                result = rows[0] as DbResponseRow;
            } break;
            default:
                throw new Error(`DbPool instance is not supported for ${clientType} database type`);
        }
    }
    else {
        const statement = await subzero.fmtTwoStepStatement(
            schema,
            prefix,
            user.role,
            req,
            queryEnv,
            o.dbMaxRows
        );
        const { query: mutate_query, parameters: mutate_parameters } = (statement as TwoStepStatement).fmtMutateStatement();
        o.debugFn && o.debugFn('env', Object.fromEntries(queryEnv));
        o.debugFn && o.debugFn('mutate query', mutate_query, mutate_parameters);
        switch (clientType) {
            case 'better-sqlite3':
                try {
                    await (db as BetterSqliteDatabase).exec('BEGIN');
                    const mutate_result = (db as BetterSqliteDatabase).prepare(mutate_query).all(mutate_parameters);
                    (statement as TwoStepStatement).setMutatedRows(mutate_result);
                    const { query: select_query, parameters: select_parameters } = (statement as TwoStepStatement).fmtSelectStatement();
                    o.debugFn && o.debugFn('select query', select_query, select_parameters);
                    result = (db as BetterSqliteDatabase).prepare(select_query).get(select_parameters) as DbResponseRow;
                    await (db as BetterSqliteDatabase).exec('COMMIT');
                }
                catch (e) {
                    await (db as BetterSqliteDatabase).exec('ROLLBACK');
                    throw e;
                }
                break;
            case 'sqlite3':
                try {
                    await (db as Sqlite3Database).exec('BEGIN');
                    const mutate_result = await (await (db as Sqlite3Database).prepare(mutate_query)).all(mutate_parameters);
                    (statement as TwoStepStatement).setMutatedRows(mutate_result);
                    const { query: select_query, parameters: select_parameters } = (statement as TwoStepStatement).fmtSelectStatement();
                    o.debugFn && o.debugFn('select query', select_query, select_parameters);
                    result = await( await (db as Sqlite3Database).prepare(select_query)).get(select_parameters) as DbResponseRow;
                    await (db as Sqlite3Database).exec('COMMIT');
                } catch (e) {
                    await (db as Sqlite3Database).exec('ROLLBACK');
                    throw e;
                }
                break;
            case 'turso': {
                const transaction = await (db as TursoDatabase).transaction('write');
                try {
                    const mutate_result = await transaction.execute({
                        sql: mutate_query,
                        args: mutate_parameters as any,
                    });
                    (statement as TwoStepStatement).setMutatedRows(mutate_result.rows);
                    const { query: select_query, parameters: select_parameters } = (statement as TwoStepStatement).fmtSelectStatement();
                    o.debugFn && o.debugFn('select query', select_query, select_parameters);
                    result = (await transaction.execute({
                        sql: select_query,
                        args: select_parameters as any,
                    })).rows[0] as DbResponseRow;
                    await transaction.commit();
                } catch (e) {
                    await transaction.rollback();
                    throw e;
                }
                }
                break;
            case 'wasm-sqlite3': {
                try {
                    (db as WasmSqlite3Database).exec('BEGIN');
                    const mutate_result: SqlValue[] = [];
                    (db as WasmSqlite3Database).exec({
                        sql: mutate_query,
                        bind: mutate_parameters as any,
                        resultRows: mutate_result,
                        rowMode: "object",
                    });
                    (statement as TwoStepStatement).setMutatedRows(mutate_result);
                    const { query: select_query, parameters: select_parameters } = (statement as TwoStepStatement).fmtSelectStatement();
                    o.debugFn && o.debugFn('select query', select_query, select_parameters);
                    const rows: SqlValue[] = [];
                    (db as WasmSqlite3Database).exec({
                        sql: select_query,
                        bind: select_parameters as any,
                        resultRows: rows,
                        rowMode: "object",
                    });
                    result = rows[0] as DbResponseRow;
                    (db as WasmSqlite3Database).exec('COMMIT');
                } catch (e) {
                    (db as WasmSqlite3Database).exec('ROLLBACK');
                    throw e;
                }
                }
                break;
            default:
                throw new Error(`DbPool instance is not supported for ${clientType} database type`);
        }
    }
    contextEnv.setEnv({});
    return result;
}

function getByteLength(str: string) {
    if (typeof Buffer !== 'undefined') {
        return Buffer.byteLength(str, 'utf8');
    }
    else if (typeof TextEncoder !== 'undefined') {
        return new TextEncoder().encode(str).length;
    }
    else {
        throw new Error('No TextEncoder implementation found');
    }
}

export function getRequestHandler(
    dbSchemas: string[],
    options: HandlerOptions = {},
) {
    const o = {
        wrapInTransaction: true,
        setDbEnv: true,
        subzeroInstanceName: '__subzero__',
        dbPoolInstanceName: '__dbPool__',
        contextEnvInstanceName: '__contextEnv__',
        dbAnonRole: 'anonymous',
        dbExtraSearchPath: ['public'],
        // eslint-disable-next-line @typescript-eslint/no-empty-function
        debugFn: () => { },
        ...options,
    }
    return async function (req: ExpressRequest, res: Response, next: NextFunction) {
        try {
            const subzero: SubzeroInternal = req.app.get(o.subzeroInstanceName);
            const dbPool: DbPool = req.app.get(o.dbPoolInstanceName);

            if (!subzero || !dbPool) {
                throw new SubzeroError('Temporary unavailable', 503);
            }
            const method = req.method || 'GET';
            if (!['GET', 'POST', 'PUT', 'DELETE', 'PATCH'].includes(method)) {
                throw new SubzeroError(`Method ${method} not allowed`, 400);
            }

            cleanupRequest(req);

            const url = new URL(`${req.protocol}://${req.get('host')}${req.originalUrl}`);
            const user = (req as any).user || { role: o.dbAnonRole };
            const header_schema = req.get('accept-profile') || req.get('content-profile');
            const { url_schema } = req.params;
            const url_schema_val = url_schema === 'rpc' ? undefined : url_schema;
            const schema = (url_schema_val || header_schema || dbSchemas[0]).toString();
            if (!dbSchemas.includes(schema)) {
                throw new SubzeroError(
                    `Schema '${schema}' not found`,
                    406,
                    `The schema must be one of the following: ${dbSchemas.join(', ')}`,
                );
            }
            // 
            const prefix = (req as any).path_prefix || '/';
            // pass env values that should be available in the query context
            // used on the query format stage
            const queryEnv: Env = [
                ['role', user.role],
                ['search_path', o.dbExtraSearchPath.join(',')],
                ['request.method', method],
                ['request.headers', JSON.stringify(req.headers)],
                ['request.get', JSON.stringify(Object.fromEntries(url.searchParams))],
                ['request.jwt.claims', JSON.stringify(user || {})],
            ];

            let result: DbResponseRow;
            switch (subzero.dbType) {
                case 'postgresql':
                    result = await restPg((dbPool as PgDatabase), subzero, req, schema, prefix, user, queryEnv, o)
                    break;
                case 'sqlite':
                    result = await restSqlite((dbPool as SqliteDatabase), subzero, req, schema, prefix, user, queryEnv, o)
                    break;
                default:
                    throw new Error(`Database type ${subzero.dbType} is not supported by express handler`);
            }
            // const result = isSqliteDatabase(dbPool) ? 
            //   await restSqlite(dbPool, subzero, req, schema, prefix, user, queryEnv, o) :
            //   await restPg(dbPool, subzero, req, schema, prefix, user, queryEnv, o)
            if (result.constraints_satisfied !== undefined && !result.constraints_satisfied) {
                throw new SubzeroError(
                    'Permission denied',
                    403,
                    'check constraint of an insert/update permission has failed',
                );
            }

            const status = Number(result.response_status) || 200;
            const pageTotal = Number(result.page_total) || 0;
            const totalResultSet = Number(result.total_result_set);
            const offset = Number(url.searchParams.get('offset') || '0') || 0;
            const response_headers = result.response_headers
                ? JSON.parse(result.response_headers)
                : {};
            response_headers['content-length'] = getByteLength(result.body || '');
            response_headers['content-type'] = 'application/json';
            response_headers['range-unit'] = 'items';
            response_headers['content-range'] = fmtContentRangeHeader(
                offset,
                offset + pageTotal - 1,
                isNaN(totalResultSet) ? undefined : totalResultSet,
            );
            res.writeHead(status, response_headers).end(result.body);
        } catch (e) {
            next(e)
        }
    };
}

export function getSchemaHandler(dbAnonRole: string, schemaInstanceName = '__schema__') {
    return async function schema(req: ExpressRequest, res: Response, next: NextFunction) {
        try {
            const schema = req.app.get(schemaInstanceName);
            if (!schema) {
                throw new SubzeroError('Temporary unavailable', 503);
            }
            const dbSchema = schema.schemas[0];
            const user = (req as any).user || { role: dbAnonRole };
            const role = user.role;
            const allowedObjects = dbSchema.objects.filter((obj: SchemaObject) => {
                const permissions = obj.permissions?.filter((permission: any) => {
                    return permission.role === role || permission.role === 'public';
                }) ?? [];
                return permissions.length > 0;
            });
            const transformedObjects = allowedObjects
                .map(({ name, kind, columns, foreign_keys }: SchemaObject) => {
                    //filter foreign keys to only include the ones that are allowed
                    const filteredForeignKeys = foreign_keys.filter((fk) => {
                        const [s, t] = fk.referenced_table;
                        if (s !== dbSchema.name) {
                            return false;
                        }
                        const o = dbSchema.objects.find((o: SchemaObject) => o.name === t);
                        if (!o) {
                            return false;
                        }
                        const permissions = o.permissions?.filter((permission: any) => {
                            return permission.role === role || permission.role === 'public';
                        }) ?? [];
                        return permissions.length > 0;
                    });
                    const transformedColumns = columns?.map((c) => {
                        return {
                            name: c.name,
                            data_type: c.data_type.toLowerCase(),
                            primary_key: c.primary_key,
                        };
                    }) ?? [];
                    return { name, kind, columns: transformedColumns, foreign_keys: filteredForeignKeys };
                })
                .reduce((acc: { [key: string]: any }, obj: SchemaObject) => {
                    acc[obj.name] = obj;
                    return acc;
                }, {});
            res.writeHead(200, { 'content-type': 'application/json' }).end(
                JSON.stringify(transformedObjects, null, 2),
            );
        } catch (e) {
            next(e)
        }
    }
}

export function getPermissionsHandler(dbAnonRole: string, schemaInstanceName = '__schema__') {
    return async function (req: ExpressRequest, res: Response, next: NextFunction) {
        try {
            const schema = req.app.get(schemaInstanceName);
            if (!schema) {
                throw new SubzeroError('Temporary unavailable', 503);
            }
            const user = (req as any).user || { role: dbAnonRole };
            const role = user.role;
            const dbSchema = schema.schemas[0];
            const allowedObjects = dbSchema.objects.filter((obj: SchemaObject) => {
                const permissions = obj.permissions?.filter((permission: any) => {
                    return permission.role === role || permission.role === 'public';
                }) ?? [];
                return permissions.length > 0;
            });
            const userPermissions = allowedObjects
                .map(({ name, kind, permissions, columns }: SchemaObject) => {
                    const userPermissions = permissions?.filter((permission: any) => {
                        return permission.role === role || permission.role === 'public';
                    }) ?? [];
                    return { name, kind, permissions: userPermissions, columns };
                })
                .reduce((acc: any[], { name, permissions }: SchemaObject) => {
                    permissions?.forEach((permission) => {
                        const { grant, columns } = permission;
                        if (!grant) {
                            // this is a RLS policy
                            return;
                        }
                        const action = grant.reduce((acc: string[], grant: string) => {
                            if (grant === 'select') {
                                acc.push('list', 'show', 'read', 'export');
                            }
                            if (grant === 'insert') {
                                acc.push('create');
                            }
                            if (grant === 'update') {
                                acc.push('edit', 'update');
                            }
                            if (grant === 'delete') {
                                acc.push('delete');
                            }
                            if (grant === 'all') {
                                acc.push('list', 'show', 'read', 'export', 'create', 'edit', 'update', 'delete');
                            }
                            return acc;
                        }, []);
                        const resource = name;
                        acc.push({ action, resource, columns: columns?.length > 0 ? columns : undefined });
                    });
                    return acc;
                }, []);

            const permissions = {
                [role]: userPermissions,
            };
            res.writeHead(200, { 'content-type': 'application/json' }).end(
                JSON.stringify(permissions, null, 2),
            );
        } catch (e) {
            next(e)
        }
    }
}

export function onSubzeroError(err: Error, req: ExpressRequest, res: Response, next: NextFunction) {
    if (err instanceof SubzeroError) {
        res.writeHead(err.status, { 'content-type': 'application/json' }).end(err.toJSONString());
    } else if (isPgError(err)) {
        const status = statusFromPgErrorCode(err.code);
        res.writeHead(status, {
            'content-type': 'application/json',
        }).end(JSON.stringify({ message: err.message, detail: err.detail, hint: err.hint }));
    } else if (isSQLiteError(err)) {
        const status = statusFromSQLiteErrorCode(err.code);
        res.writeHead(status, {
            'content-type': 'application/json',
        }).end(JSON.stringify({ message: err.message }));
    } else {
        next(err);
    }
}

function toSubzeroError(err: any) {
    const wasm_err: string = err.message
    try {
        const ee = JSON.parse(wasm_err)
        return new SubzeroError(ee.message, ee.status, ee.description)
    } catch (e) {
        return new SubzeroError(wasm_err)
    }
}

export class TwoStepStatement {
    private mutate: Statement
    private select: Statement
    private ids?: string[]
    constructor(mutate: Statement, select: Statement) {
        this.mutate = mutate
        this.select = select
    }

    fmtMutateStatement(): Statement {
        return this.mutate
    }

    fmtSelectStatement(): Statement {
        // check the ids are set
        if (!this.ids) {
            throw new Error('ids of the mutated rows are not set')
        }
        const { query, parameters } = this.select
        const placeholder = '["_subzero_ids_placeholder_"]'
        // replace placeholder with the actual ids in json format
        parameters.forEach((p, i) => {
            if (p == placeholder) {
                parameters[i] = JSON.stringify(this.ids)
            }
        })
        return { query, parameters }
    }

    setMutatedRows(rows: any[]) {
        const constraints_satisfied = rows.every((r) =>
            r['_subzero_check__constraint'] !== undefined ? r['_subzero_check__constraint'] == 1 : true
        )
        if (constraints_satisfied) {
            const rowColumns = rows[0] ? Object.keys(rows[0]) : []
            const idColumnName = rowColumns.length > 0 ? rowColumns[0] : undefined
            const ids = rows.map((r) => idColumnName ? r[idColumnName] : r)
            this.ids = ids
        }
        else {
            throw new SubzeroError('Permission denied', 403, 'check constraint of an insert/update permission has failed')
        }
    }
}

type RequestParts = {
    parsedUrl: URL,
    textBody: string,
    headersSequence: [string, string | undefined][],
    //headersSequence: any
}

// function isExpressRequest(obj: any): obj is Request {
//   return 'app' in obj && typeof obj.app === 'function';
// }

async function readRequestBody(req: IncomingMessage): Promise<string> {
    return new Promise((resolve, reject) => {
        let body = '';

        req.on('data', chunk => {
            body += chunk;
        });

        req.on('end', () => {
            resolve(body);
        });

        req.on('error', err => {
            reject(err);
        });
    });
}


async function getRequestParts(req: Request | ExpressRequest | IncomingMessage): Promise<RequestParts> {
    // try to accommodate for different request types
    let body: string | undefined = undefined;
    if (typeof Request !== 'undefined' && req instanceof Request) {
        return {
            parsedUrl: new URL(req.url),
            textBody: req.method === 'GET' ? '' : await req.text(),
            headersSequence: Array.from(req.headers.entries()).map(([k, v]) => [k.toLowerCase(), v?.toString()])
        }
    }
    // this is either express or IncomingMessage (which are quite similar)
    else {
        const r = req as IncomingMessage;
        body = (r as any).body || (r as any).textBody
        // read the body if it's not read yet
        if (!body) {
            body = await readRequestBody(r)
        }
        else if (typeof body === 'object') {
            body = JSON.stringify(body)
        }
        return {
            parsedUrl: new URL(r.url || '', `http://${r.headers.host}`),
            textBody: r.method === 'GET' ? '' : body || '',
            headersSequence: Object.entries(r.headers).map(([k, v]) => [k.toLowerCase(), v?.toString()])
        }
    }
}


export class SubzeroInternal {
    private backend?: any
    private wasmBackend: any
    private wasmPromise?: Promise<any>
    private wasmInitialized = false
    public dbType: DbType
    private schema: any
    private allowed_select_functions?: string[]
    private licenseKey?: string
    //private wasmInitialized = false

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    constructor(wasmBackend: any, dbType: DbType, schema: any, allowed_select_functions?: string[], wasmPromise?: Promise<any>, licenseKey?: string) {
        this.dbType = dbType
        this.allowed_select_functions = allowed_select_functions
        this.schema = schema
        this.wasmBackend = wasmBackend
        this.wasmPromise = wasmPromise
        this.licenseKey = licenseKey
        if (!this.wasmPromise) {
            this.wasmInitialized = true
            this.initBackend()
        }
    }

    // async init(wasmPromise: Promise<any>) {
    //   if (!this.wasmInitialized) {
    //     await wasmPromise
    //     this.wasmInitialized = true
    //   }
    //   await this.initBackend()
    // }
    private initBackend(force = false) {
        if (this.backend && !force) {
            return
        }
        if (!this.wasmInitialized) {
            throw new Error('WASM not initialized')
        }
        try {
            this.backend = this.wasmBackend.init(JSON.stringify(this.schema, null, 2), this.dbType, this.allowed_select_functions, this.licenseKey)
        } catch (e: any) {
            throw toSubzeroError(e)
        }
    }
    async init() {
        if (!this.wasmInitialized) {
            await this.wasmPromise
            this.wasmInitialized = true
        }
        this.initBackend()
    }

    setSchema(schema: any) {
        this.schema = schema
        this.initBackend(true)
    }


    // private async normalizeRequest(request: HttpRequest): Promise<RequestParts> {
    //   // try to accommodate for different request types

    //   if (typeof Request !== 'undefined' && request instanceof Request) {
    //     const r = request as Request;
    //     return {
    //       parsedUrl: new URL(r.url || '', `http://${r.headers.host}`),
    //       textBody: request.method === 'GET' ? '' : await r.text(),
    //       headersSequence: r.headers,
    //     }
    //     request.parsedUrl = new URL(request.url)
    //     request.textBody = request.method === 'GET' ? '' : await request.text()
    //     request.headersSequence = request.headers
    //   }
    //   // check if type is IncomingMessage
    //   else {
    //     request = request as IncomingMessage;
    //     request.parsedUrl = new URL(request.url || '', `http://${request.headers.host}`)
    //     request.headersSequence = Object.entries(request.headers).map(([k, v]) => [k.toLowerCase(), v?.toString()])
    //     if (request.method === 'GET') {
    //       request.textBody = ''
    //     }
    //     else {
    //       if (!request.body) {
    //         // the body was not read yet
    //         if (typeof request.text === 'function') {
    //           request.textBody = await request.text()
    //         }
    //         else {
    //           request.textBody = ''
    //         }
    //       }
    //       else if (typeof request.body === 'object') {
    //         request.textBody = JSON.stringify(request.body)
    //       }
    //     }
    //   }
    // }

    async fmtStatement(schemaName: string, urlPrefix: string, role: string, request: Request | ExpressRequest | IncomingMessage, env: Env, maxRows?: number,): Promise<Statement> {
        try {
            if (!this.backend) {
                throw new Error('Subzero is not initialized')
            }
            const p = await getRequestParts(request)
            //const parsedUrl = request.parsedUrl || new URL('');
            const maxRowsStr = maxRows !== undefined ? maxRows.toString() : undefined;
            const [query, parameters] = this.backend.fmt_main_query(
                schemaName,
                p.parsedUrl.pathname.substring(urlPrefix.length) || '', // entity
                request.method || 'GET', // method
                p.parsedUrl.pathname, // path
                p.parsedUrl.searchParams, // get
                p.textBody, // body
                role,
                p.headersSequence,
                [], //cookies
                env,
                maxRowsStr
            )
            return { query, parameters }
        } catch (e) {
            throw toSubzeroError(e)
        }
    }

    async fmtTwoStepStatement(schemaName: string, urlPrefix: string, role: string, request: Request | ExpressRequest | IncomingMessage, env: Env, maxRows?: number,): Promise<TwoStepStatement> {
        try {
            if (!this.backend) {
                throw new Error('Subzero is not initialized')
            }
            //await this.normalizeRequest(request);
            //const parsedUrl = request.parsedUrl || new URL('');
            const p = await getRequestParts(request)
            const maxRowsStr = maxRows !== undefined ? maxRows.toString() : undefined;

            const [mutate_query, mutate_parameters, select_query, select_parameters] = this.backend.fmt_two_stage_query(
                schemaName,
                p.parsedUrl.pathname.substring(urlPrefix.length) || '', // entity
                request.method || 'GET', // method
                p.parsedUrl.pathname, // path
                p.parsedUrl.searchParams, // get
                p.textBody, // body
                role,
                p.headersSequence,
                [], //cookies
                env,
                maxRowsStr
            )
            return new TwoStepStatement({ query: mutate_query, parameters: mutate_parameters }, { query: select_query, parameters: select_parameters })
        } catch (e: any) {
            throw toSubzeroError(e)
        }
    }

}

export function getRawIntrospectionQuery(dbType: DbType): Query {
    switch (dbType) {
        case 'postgresql':
            return postgresql_introspection_query
        case 'sqlite':
            return sqlite_introspection_query
        case 'clickhouse':
            return clickhouse_introspection_query
        case 'mysql':
            return mysql_introspection_query
        default:
            throw new Error(`Unknown dbType: ${dbType}`)
    }
    // return fs.readFileSync(path.join(__dirname, `../introspection/${dbType}_introspection_query.sql`), 'utf8')
}

/**
 * subzero allows to define custom permissions and relations in the schema through json files.
 * For that purpose, the introspection queries need a mechanism to include the contents of those files.
 * The raw queries are templates with placeholders like {@filename#default_missing_value} that are replaced by the contents of the file.
 * an example placeholder looks like {@permissions.json#[]}
 * The following function replaces the placeholders with the contents of the files if they exist.
 */
export function getIntrospectionQuery(
    dbType: DbType,
    schemas: string | string[],
    placeholder_values?: Map<string, any>,
    includeAllDbRoles = false,
): Statement {
    const re = /'\[\]'--([a-zA-Z0-9_.]+\.json)/g;
    const placeholder_values_map = placeholder_values || new Map<string, any>()
    const raw_query = getRawIntrospectionQuery(dbType)
    
    const query = raw_query.replace(re, (match, filename) => {
        if (placeholder_values_map.has(filename)) {
            return `'${JSON.stringify(placeholder_values_map.get(filename))}'`;
        } else {
            return "'[]'";
        }
    });
    const parameters: (string | string[] | boolean | number)[] = typeof schemas === 'string' ? [[schemas]] : [schemas]
    if (dbType === 'sqlite' || dbType === 'mysql') {
        parameters[0] = [JSON.stringify(parameters[0])]
    }
    if (dbType === 'postgresql') {
        parameters.push(includeAllDbRoles)
    }
    return { query, parameters }
}

export function parseRangeHeader(headerValue: string): { first: number; last: number, total: number } {
    const parts = headerValue.split('/')
    const total = parseInt(parts[1], 10) || 0
    const range = parts[0].split('-')
    const first = parseInt(range[0], 10) || 0
    const last = parseInt(range[1], 10) || 0
    return { first, last, total }
}

// helper function to format the value of the content-range header (ex: 0-9/100)
export function fmtContentRangeHeader(lower: number, upper: number, total?: number): string {
    const range_string = (total !== undefined && total != 0 && lower <= upper) ? `${lower}-${upper}` : '*'
    return total !== undefined ? `${range_string}/${total}` : `${range_string}/*`
}

export function fmtPostgreSqlEnv(env: Env): Statement {
    if (env.length === 0) {
        env.push(['subzero._dummy_', 'true'])
    }

    const parameters = env.flat()

    const query = 'select ' + parameters.reduce((acc: string[], _, i) => {
        if (i % 2 !== 0) {
            acc.push(`set_config($${i}, $${i + 1}, true)`)
        }
        return acc
    }
        , []
    ).join(', ')
    return { query, parameters }
}

export function fmtMySqlEnv(env: Env): Statement {
    env.push(['subzero_ids', '[]'])
    env.push(['subzero_ignored_ids', '[]'])
    const parameters: any[] = []
    const queryParts: string[] = []
    env.forEach(([key, value]) => {
        queryParts.push(`@${key} = ?`)
        parameters.push(value)
    })
    const query = `set ${queryParts.join(', ')}`
    return { query, parameters }
}

export function statusFromPgErrorCode(code: string, authenticated = false): number {
    let responseCode
    switch (true) {
        case /^08/.test(code): responseCode = 503; break;            // pg connection err
        case /^09/.test(code): responseCode = 500; break;            // triggered action exception
        case /^0L/.test(code): responseCode = 403; break;            // invalid grantor
        case /^0P/.test(code): responseCode = 403; break;            // invalid role specification
        case /^23503/.test(code): responseCode = 409; break;         // foreign_key_violation
        case /^23505/.test(code): responseCode = 409; break;         // unique_violation
        case /^25006/.test(code): responseCode = 405; break;         // read_only_sql_transaction
        case /^25/.test(code): responseCode = 500; break;            // invalid tx state
        case /^28/.test(code): responseCode = 403; break;            // invalid auth specification
        case /^2D/.test(code): responseCode = 500; break;            // invalid tx termination
        case /^38/.test(code): responseCode = 500; break;            // external routine exception
        case /^39/.test(code): responseCode = 500; break;            // external routine invocation
        case /^3B/.test(code): responseCode = 500; break;            // savepoint exception
        case /^40/.test(code): responseCode = 500; break;            // tx rollback
        case /^53/.test(code): responseCode = 503; break;            // insufficient resources
        case /^54/.test(code): responseCode = 413; break;            // too complex
        case /^55/.test(code): responseCode = 500; break;            // obj not on prereq state
        case /^57/.test(code): responseCode = 500; break;            // operator intervention
        case /^58/.test(code): responseCode = 500; break;            // system error
        case /^F0/.test(code): responseCode = 500; break;            // conf file error
        case /^HV/.test(code): responseCode = 500; break;            // foreign data wrapper error
        case /^P0001/.test(code): responseCode = 400; break;         // default code for "raise"
        case /^P0/.test(code): responseCode = 500; break;            // PL/pgSQL Error
        case /^XX/.test(code): responseCode = 500; break;            // internal Error
        case /^42883/.test(code): responseCode = 404; break;         // undefined function
        case /^42P01/.test(code): responseCode = 404; break;         // undefined table
        case /^42501/.test(code): responseCode = authenticated ? 403 : 401; break; // insufficient privilege{
        case /^PT/.test(code): responseCode = Number(code.substr(2, 3)) || 500; break;
        default: responseCode = 400; break;
    }

    return responseCode
}

export function statusFromSQLiteErrorCode(code: string, authenticated = false): number {
    let responseCode;
    switch (true) {
        case /SQLITE_ABORT/.test(code):
            responseCode = 503; break;
        case /SQLITE_AUTH/.test(code):
            responseCode = authenticated ? 403 : 401; break;
        case /SQLITE_BUSY/.test(code):
            responseCode = 503; break;
        case /SQLITE_CANTOPEN/.test(code):
            responseCode = 500; break;
        case /SQLITE_CONSTRAINT/.test(code):
            responseCode = 409; break;
        case /SQLITE_CORRUPT/.test(code):
        case /SQLITE_NOTADB/.test(code):
            responseCode = 500; break;
        case /SQLITE_ERROR/.test(code):
            responseCode = 400; break;
        case /SQLITE_FULL/.test(code):
            responseCode = 507; break;
        case /SQLITE_IOERR/.test(code):
            responseCode = 500; break;
        case /SQLITE_LOCKED/.test(code):
            responseCode = 423; break;
        case /SQLITE_MISMATCH/.test(code):
            responseCode = 409; break;
        case /SQLITE_MISUSE/.test(code):
            responseCode = 500; break;
        case /SQLITE_NOMEM/.test(code):
            responseCode = 507; break;
        case /SQLITE_PERM/.test(code):
            responseCode = 403; break;
        case /SQLITE_READONLY/.test(code):
            responseCode = 403; break;
        case /SQLITE_TOOBIG/.test(code):
            responseCode = 413; break;
        // Extended constraint-related result codes
        case /SQLITE_CONSTRAINT_CHECK/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_COMMITHOOK/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_FOREIGNKEY/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_NOTNULL/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_PRIMARYKEY/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_TRIGGER/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_UNIQUE/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_ROWID/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_VTAB/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_PINNED/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_FUNCTION/.test(code):
            responseCode = 409; break;
        case /SQLITE_CONSTRAINT_DATATYPE/.test(code):
            responseCode = 409; break;
        default:
            responseCode = 400; break;
    }

    return responseCode;
}

