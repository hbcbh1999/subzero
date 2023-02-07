import {default as sqlite_introspection_query } from '../introspection/sqlite_introspection_query.sql'
import {default as postgresql_introspection_query } from '../introspection/postgresql_introspection_query.sql'
import {default as clickhouse_introspection_query } from '../introspection/clickhouse_introspection_query.sql'

import { default as wasmbin } from '../../subzero-wasm/pkg/subzero_wasm_bg.wasm'
import /*init, */{ initSync, Backend } from '../../subzero-wasm/pkg/subzero_wasm.js'
import type { IncomingMessage } from 'http'
import type { NextApiRequest } from 'next'
import type { Request as ExpressRequest } from 'express'
import type { Request as KoaRequest } from 'koa'

type HttpRequest = Request | IncomingMessage | NextApiRequest | ExpressRequest | KoaRequest
type SubzeroHttpRequest = HttpRequest & {
  parsedUrl?: URL,
  textBody?: string,
  body?: unknown,
  headersSequence?: unknown,
}
/* tslint:disable */
/* eslint-disable */
initSync(wasmbin)
//init(wasmbin)

export type DbType = 'postgresql' | 'sqlite' | 'clickhouse' | 'mysql'
export type Query = string
export type Parameters = (string | number | boolean | null | (string | number | boolean | null)[])[]
export type Statement = { query: Query, parameters: Parameters }

export type GetParameters = [string, string][]
export type Method = 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH'
export type Body = string | undefined
export type Headers = [string, string][]
export type Cookies = [string, string][]
export type Env = [string, string][]

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
    let { message, description } = this
    return JSON.stringify({ message, description })
  }
}

function toSubzeroError(err: any) {
  let wasm_err: string = err.message
  try {
    let ee: any = JSON.parse(wasm_err)
    return new SubzeroError(ee.message, ee.status, ee.description)
  } catch(e) {
    return new SubzeroError(wasm_err)
  }
}

export class SqliteTwoStepStatement {
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
    let { query, parameters } = this.select
    let placeholder = '["_subzero_ids_placeholder_"]'
    // replace placeholder with the actual ids in json format
    parameters.forEach((p, i) => {
      if (p == placeholder) {
        parameters[i] = JSON.stringify(this.ids)
      }
    })
    return { query, parameters }
  }

  setMutatedRows(rows: any[]) {
    const constraints_satisfied = rows.every((r) => r['_subzero_check__constraint'] == 1)
    if (constraints_satisfied) {
      let idColumnName = rows[0] ? Object.keys(rows[0])[0] : ''
      const ids = rows.map((r) => r[idColumnName].toString())
      this.ids = ids
    }
    else {
        throw new SubzeroError('Permission denied', 403, 'check constraint of an insert/update permission has failed')
    }
  }
}

export class Subzero {
  private backend: Backend
  private dbType: DbType
  private allowed_select_functions?: string[]

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
    try {
      this.dbType = dbType
      this.allowed_select_functions = allowed_select_functions
      this.backend = Backend.init(JSON.stringify(schema), dbType, allowed_select_functions)
    } catch (e:any) {
      throw toSubzeroError(e)
    }
  }

  setSchema(schema: any) {
    try {
      this.backend = Backend.init(JSON.stringify(schema), this.dbType, this.allowed_select_functions)
    } catch (e: any) {
      throw toSubzeroError(e)
    }
  }

  private async normalizeRequest(request: SubzeroHttpRequest): Promise<void> {
    // try to accomodate for different request types

    if (request instanceof Request) {
      // @ts-ignore
      request.parsedUrl = new URL(request.url)
      // @ts-ignore
      request.textBody = request.method === 'GET' ? '' : await request.text()
      // @ts-ignore
      request.headersSequence = request.headers
    }
    else {
      request.parsedUrl = new URL(request.url || '', `http://${request.headers.host}`)
      request.headersSequence = Object.entries(request.headers)
      if (request.method === 'GET') {
        request.textBody = ''
      }
      else {
        if (!request.body) {
          // the body was not read yet
          // @ts-ignore
          if (typeof request.text === 'function') {
            // @ts-ignore
            request.textBody = await request.text()
          }
          else {
            request.textBody = ''
          }
        }
        else if (typeof request.body === 'object') {
          request.textBody = JSON.stringify(request.body)
        }
      }
    }
  }

  async fmtStatement(schemaName: string, urlPrefix: string, role: string, request: SubzeroHttpRequest,  env: Env, maxRows?: number,): Promise<Statement> {
    try {
      await this.normalizeRequest(request);
      let parsedUrl = request.parsedUrl || new URL('');
      let maxRowsStr = maxRows !== undefined ? maxRows.toString() : undefined;
      
      const [query, parameters] = this.backend.fmt_main_query(
            schemaName,
            parsedUrl.pathname.substring(urlPrefix.length) || '', // entity
            request.method || 'GET', // method
            parsedUrl.pathname, // path
            parsedUrl.searchParams, // get
            request.textBody !== undefined ? request.textBody : (request.body || ''), // body
            role,
            request.headersSequence,
            [], //cookies
            env,
            maxRowsStr
      )
      return { query, parameters }
    } catch (e: any) {
      throw toSubzeroError(e)
    }
  }

  async fmtSqliteTwoStepStatement(schemaName: string, urlPrefix: string, role: string, request: SubzeroHttpRequest,  env: Env, maxRows?: number,): Promise<SqliteTwoStepStatement> {
    try {
      await this.normalizeRequest(request);
      let parsedUrl = request.parsedUrl || new URL('');
      let maxRowsStr = maxRows !== undefined ? maxRows.toString() : undefined;
      
      const [mutate_query, mutate_parameters, select_query, select_parameters] = this.backend.fmt_sqlite_two_stage_query(
            schemaName,
            parsedUrl.pathname.substring(urlPrefix.length) || '', // entity
            request.method || 'GET', // method
            parsedUrl.pathname, // path
            parsedUrl.searchParams, // get
            request.textBody !== undefined ? request.textBody : (request.body || ''), // body
            role,
            request.headersSequence,
            [], //cookies
            env,
            maxRowsStr
      )
      return new SqliteTwoStepStatement({ query: mutate_query, parameters: mutate_parameters }, { query: select_query, parameters: select_parameters })
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
  const parameters = typeof schemas === 'string' ? [[schemas]] : [schemas]
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
  const range_string = (total != 0 && lower <= upper) ? `${lower}-${upper}` : '*'
  return total ? `${range_string}/${total}` : `${range_string}/*`
}

export function fmtPostgreSqlEnv(env: Env): Statement {
  let parameters = env.flat()
  let query = 'select ' + parameters.reduce((acc:string[], _, i) => {
      if (i % 2 !== 0) {
        acc.push(`set_config(\$${i}, \$${i+1}, true)`)
      }
      return acc
    }
    , []
  ).join(', ')
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
