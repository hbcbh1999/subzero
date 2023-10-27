import { default as sqlite_introspection_query } from '../introspection/sqlite_introspection_query.sql'
import { default as postgresql_introspection_query } from '../introspection/postgresql_introspection_query.sql'
import { default as clickhouse_introspection_query } from '../introspection/clickhouse_introspection_query.sql'
import { default as mysql_introspection_query } from '../introspection/mysql_introspection_query.sql'
import type { Pool as PgPool } from 'pg'
import type { Database as SqliteDatabase } from 'better-sqlite3'
import type { Request as ExpressRequest, NextFunction, Response } from 'express'
import type { IncomingMessage } from 'http'
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
export type DbPool = PgPool | SqliteDatabase
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
  statusCode():number {
      return this.status
  }
  toJSONString():string {
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

export function isPgPool(pool: DbPool): pool is PgPool {
  return (pool as PgPool).query !== undefined;
}

export function isSqliteDatabase(pool: DbPool): pool is SqliteDatabase {
  return (pool as SqliteDatabase).prepare !== undefined;
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
  if (req.headers['prefer'] === '') {
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
  status?: number,
  body?: string,
  page_total?: number,
  total_result_set?: number,
  constraints_satisfied?: boolean,
  response_headers?: string,
}

async function restPg(dbPool: PgPool, subzero: SubzeroInternal,
  req: ExpressRequest,
  schema: string,
  prefix: string,
  user: any,
  queryEnv: Env,
  o: HandlerOptions):Promise<DbResponseRow> {

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
  o: HandlerOptions):Promise<DbResponseRow> {
  const contextEnv: ContextEnv | undefined = req.app.get(o.contextEnvInstanceName as string);
  if (!contextEnv) {
    throw new SubzeroError('Context Env for sqlite not set', 500);
  }
  let transactionStarted = false;
  const db = dbPool;
  try {
      // generate the SQL query from request object
      const method = req.method || 'GET';
      let statement: Statement | TwoStepStatement;
      if (method == 'GET') {
          statement = await subzero.fmtStatement(
              schema,
              prefix,
              user.role,
              req,
              queryEnv,
              o.dbMaxRows
          );
      }
      else {
          statement = await subzero.fmtTwoStepStatement(
              schema,
              prefix,
              user.role,
              req,
              queryEnv,
              o.dbMaxRows
          );
      }

      if (o.wrapInTransaction) {
          db.exec('BEGIN'); 
          transactionStarted = true;
      }
      let result: DbResponseRow;
      if (method == 'GET' && statement) {
        const { query, parameters } = statement as Statement;
        o.debugFn && o.debugFn('env', Object.fromEntries(queryEnv));
        o.debugFn && o.debugFn('main query', query, parameters);
        contextEnv.setEnv(Object.fromEntries(queryEnv));
        const stm = db.prepare(query)
        result = stm.get(parameters) as DbResponseRow;
        contextEnv.setEnv({});
      }
      else {
          const { query: mutate_query, parameters: mutate_parameters } = (statement as TwoStepStatement).fmtMutateStatement();
          o.debugFn && o.debugFn('env', Object.fromEntries(queryEnv));
          o.debugFn && o.debugFn('mutate query', mutate_query, mutate_parameters);
          contextEnv.setEnv(Object.fromEntries(queryEnv));
          const mutate_result = db.prepare(mutate_query).all(mutate_parameters);
          (statement as TwoStepStatement ).setMutatedRows(mutate_result);
          const { query: select_query, parameters: select_parameters } = (statement as TwoStepStatement).fmtSelectStatement();
          o.debugFn && o.debugFn('select query', select_query, select_parameters);
          result = db.prepare(select_query).get(select_parameters) as DbResponseRow;
          contextEnv.setEnv({});
      }
      db.exec('COMMIT');
      return result;
      
  } catch (e) {
      if (o.wrapInTransaction && transactionStarted) {
        db.exec('ROLLBACK');
      }
      throw e;
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
    debugFn: () => {},
    ...options,
  }
  return async function (req: ExpressRequest, res: Response, next: NextFunction) {
      try {
        const subzero:SubzeroInternal = req.app.get(o.subzeroInstanceName);
        const dbPool:DbPool = req.app.get(o.dbPoolInstanceName);

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
        const header_schema = req.headers['accept-profile'] || req.headers['content-profile'];
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
        const prefix = '/';
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
      
        const result = isSqliteDatabase(dbPool) ? 
          await restSqlite(dbPool, subzero, req, schema, prefix, user, queryEnv, o) :
          await restPg(dbPool, subzero, req, schema, prefix, user, queryEnv, o)
        if (result.constraints_satisfied !== undefined && !result.constraints_satisfied) {
            throw new SubzeroError(
                'Permission denied',
                403,
                'check constraint of an insert/update permission has failed',
            );
        }
        
        const status = Number(result.status) || 200;
        const pageTotal = Number(result.page_total) || 0;
        const totalResultSet = Number(result.total_result_set);
        const offset = Number(url.searchParams.get('offset') || '0') || 0;
        const response_headers = result.response_headers
            ? JSON.parse(result.response_headers)
            : {};
        response_headers['content-length'] = Buffer.byteLength(result.body || '');
        response_headers['content-type'] = 'application/json';
        response_headers['range-unit'] = 'items';
        response_headers['content-range'] = fmtContentRangeHeader(
            offset,
            offset + pageTotal - 1,
            isNaN(totalResultSet)? undefined : totalResultSet,
        );
        res.writeHead(status, response_headers).end(result.body);
      } catch (e) {
        next(e)
      }
  };
}

export function getSchemaHandler(dbAnonRole:string, schemaInstanceName = '__schema__') {
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
        const permissions = obj.permissions.filter((permission: any) => {
          return permission.role === role || permission.role === 'public';
        });
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
            const permissions = o.permissions.filter((permission: any) => {
              return permission.role === role || permission.role === 'public';
            });
            return permissions.length > 0;
          });
          const transformedColumns = columns.map((c) => {
            return {
              name: c.name,
              data_type: c.data_type.toLowerCase(),
              primary_key: c.primary_key,
            };
          });
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
        const permissions = obj.permissions.filter((permission: any) => {
          return permission.role === role || permission.role === 'public';
        });
        return permissions.length > 0;
      });
      const userPermissions = allowedObjects
        .map(({ name, kind, permissions, columns }: SchemaObject) => {
          const userPermissions = permissions.filter((permission: any) => {
            return permission.role === role || permission.role === 'public';
          });
          return { name, kind, permissions: userPermissions, columns };
        })
        .reduce((acc: any[], { name, permissions}: SchemaObject) => {
          permissions.forEach((permission) => {
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
            acc.push({ action, resource, columns: columns && columns.length > 0 ? columns : undefined });
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
  } else {
      next(err);
  }
}

function toSubzeroError(err: any) {
  const wasm_err: string = err.message
  try {
    const ee = JSON.parse(wasm_err)
    return new SubzeroError(ee.message, ee.status, ee.description)
  } catch(e) {
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
  private dbType: DbType
  private schema: any
  private allowed_select_functions?: string[]
  //private wasmInitialized = false

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(wasmBackend: any, dbType: DbType, schema: any, allowed_select_functions?: string[], wasmPromise?: Promise<any>) {
    this.dbType = dbType
    this.allowed_select_functions = allowed_select_functions
    this.schema = schema
    this.wasmBackend = wasmBackend
    this.wasmPromise = wasmPromise
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
  private initBackend() {
    if (!this.wasmInitialized) {
      throw new Error('WASM not initialized')
    }
    try {
      this.backend = this.wasmBackend.init(JSON.stringify(this.schema), this.dbType, this.allowed_select_functions)
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
    this.initBackend()
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

  async fmtStatement(schemaName: string, urlPrefix: string, role: string, request: Request | ExpressRequest | IncomingMessage,  env: Env, maxRows?: number,): Promise<Statement> {
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

  async fmtTwoStepStatement(schemaName: string, urlPrefix: string, role: string, request: Request | ExpressRequest | IncomingMessage,  env: Env, maxRows?: number,): Promise<TwoStepStatement> {
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  placeholder_values?: Map<string, any>,
  includeAllDbRoles = false,
): Statement {
  const re = new RegExp(`{@([^#}]+)(#([^}]+))?}`, 'g')
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const placeholder_values_map = placeholder_values || new Map<string, any>()
  const raw_query = getRawIntrospectionQuery(dbType)
  const parts = raw_query.split(re)
  let query = ''
  for (let i = 0; i < parts.length; i += 4) {
    query += parts[i]

    if (i + 1 < parts.length) {
      const file_to_include = parts[i + 1]
      let default_value = parts[i + 3]
      if (default_value === undefined) {
        default_value = `{not found @${file_to_include}}`
      }
      if (placeholder_values_map.has(file_to_include)) {
        query += JSON.stringify(placeholder_values_map.get(file_to_include))
      // } else if (fs.existsSync(file_to_include)) {
      //   const file_content = fs.readFileSync(file_to_include, 'utf8')
      //   query += file_content
      } else {
        query += default_value
      }
    }
  }
  const parameters:(string | string[] | boolean | number)[] = typeof schemas === 'string' ? [[schemas]] : [schemas]
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
  const total = parseInt(parts[1], 10) ||  0
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
  
  const query = 'select ' + parameters.reduce((acc:string[], _, i) => {
      if (i % 2 !== 0) {
        acc.push(`set_config($${i}, $${i+1}, true)`)
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
  const parameters:any[] = []
  const queryParts:string[] = []
  env.forEach(([key,value]) => {
      queryParts.push(`@${key} = ?`)
      parameters.push(value)
  })
  const query = `set ${queryParts.join(', ')}`
  return { query, parameters }
}

export function statusFromPgErrorCode(code: string, authenticated = false) : number {
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
        case /^42501/.test(code): responseCode = authenticated?403:401; break; // insufficient privilege{
        case /^PT/.test(code): responseCode = Number(code.substr(2,3)) || 500; break;
        default: responseCode = 400; break;
    }

    return responseCode
}
