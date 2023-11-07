import { Backend } from '../../subzero-wasm/pkg-node/subzero_wasm.js'
import {
    DbType, SubzeroInternal, DbPool, InitOptions,
    getIntrospectionQuery,
    isPgPool, isSqliteDatabase, ContextEnv, onSubzeroError,
} from './subzero'
import type { Express} from 'express'
export default class Subzero extends SubzeroInternal {
    constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
        super(Backend, dbType, schema, allowed_select_functions)
    }
}
export type {
    DbType,
    DbPool,
    Query,
    Parameters,
    Statement,
    GetParameters,
    Method,
    Body,
    Headers,
    Cookies,
    Env,
    SchemaColumn, SchemaForeignKey, SchemaObject, Schema,
    InitOptions,
    HandlerOptions,
} from './subzero'

export {
    TwoStepStatement,
    SubzeroError,
    fmtContentRangeHeader,
    fmtPostgreSqlEnv,
    fmtMySqlEnv,
    getIntrospectionQuery,
    getRawIntrospectionQuery,
    parseRangeHeader,
    statusFromPgErrorCode,
    getRequestHandler, getSchemaHandler, getPermissionsHandler, onSubzeroError,
} from './subzero'

export async function init(
    app: Express,
    dbType: DbType,
    dbPool: DbPool,
    dbSchemas: string[],
    options: InitOptions = {},
): Promise<Subzero | undefined> {

    let subzero: Subzero| undefined = undefined;
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
            if (dbType === 'postgresql' && isPgPool(dbPool)) {
                const result = await dbPool.query(query, parameters)
                schema = JSON.parse(result.rows[0].json_schema)
            } else if (dbType === 'sqlite' && isSqliteDatabase(dbPool)) {
                const result: any = dbPool.prepare(query).get();
                schema = JSON.parse(result.json_schema);
            } else {
                throw new Error(`Database type ${dbType} is not supported`)
            }
            schema.use_internal_permissions = o.useInternalPermissionsCheck;
            const json = JSON.stringify(schema, null, 2);
            const withLineNumbers = json.split('\n').map((line, index) => {
                return `${(index + 1).toString().padStart(4, ' ')}: ${line}`;
            }).join('\n');
            o.debugFn("schema:\n", withLineNumbers);
            subzero = new Subzero(dbType, schema, o.allowedSelectFunctions);
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

    app.use(onSubzeroError);
    app.set(o.subzeroInstanceName, subzero);
    app.set(o.dbPoolInstanceName, dbPool);
    if (dbType === 'sqlite' && isSqliteDatabase(dbPool)) {
        const contextEnv = new ContextEnv();
        const boundGetEnvVar = contextEnv.getEnvVar.bind(contextEnv);
        dbPool.function('env', boundGetEnvVar as (...params: unknown[]) => unknown);
        const boundJwt = contextEnv.jwt.bind(contextEnv);
        dbPool.function('jwt', boundJwt as () => unknown);
        app.set(o.contextEnvInstanceName, contextEnv);
    }
    return subzero;
}
