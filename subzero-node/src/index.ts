import {default as sqlite_introspection_query } from '../introspection/sqlite_introspection_query.sql'
import {default as postgresql_introspection_query } from '../introspection/postgresql_introspection_query.sql'
import {default as clickhouse_introspection_query } from '../introspection/clickhouse_introspection_query.sql'

import { default as wasmbin } from '../../subzero-wasm/pkg/subzero_wasm_bg.wasm'
import /*init, */{ initSync, Backend } from '../../subzero-wasm/pkg/subzero_wasm.js'
import type { Request as SubzeroRequest } from '../../subzero-wasm/pkg/subzero_wasm.js'
export type { Request } from '../../subzero-wasm/pkg/subzero_wasm.js'
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

export type DbType = 'postgresql' | 'sqlite' | 'clickhouse'
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

// export async function init_wasm() {
//   await init()
// }
function toSubzeroError(err: any) {
  let wasm_err: string = err.message
  try {
    let ee: any = JSON.parse(wasm_err)
    return new SubzeroError(ee.message, ee.status, ee.description)
  } catch(e) {
    return new SubzeroError(wasm_err)
  }
}

export class Subzero {
  private backend: Backend

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
    try {
      this.backend = Backend.init(JSON.stringify(schema), dbType, allowed_select_functions)
    } catch (e:any) {
      throw toSubzeroError(e)
    }
  }

  async parse(schemaName: string, urlPrefix: string, role: string, request: SubzeroHttpRequest): Promise<SubzeroRequest> {
    // try to accomodate for different request types

    if (request instanceof Request) {
      request.parsedUrl = new URL(request.url)
      request.textBody = request.method === 'GET' ? '' : await request.text()
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

    try {
      return this.backend.parse(
        schemaName,
        request.parsedUrl.pathname.substring(urlPrefix.length), // entity
        request.method || 'GET', // method
        request.parsedUrl.pathname, // path
        request.parsedUrl.searchParams, // get
        request.textBody !== undefined ? request.textBody : (request.body || ''), // body
        role,
        request.headersSequence,
        [] // cookies
      )
    } catch (e: any) {
      throw toSubzeroError(e)
    }
  }

  fmtMainQuery(request: SubzeroRequest, env: Env): Statement {
    try {
      const [query, parameters] = this.backend.fmt_main_query(request, env)
      //const query = _query.replaceAll('rarray(', 'carray(')
      return { query, parameters }
    } catch (e: any) {
      throw toSubzeroError(e)
    }
  }

  fmtSqliteMutateQuery(request: SubzeroRequest, env: Env): Statement {
    try {
      const [query, parameters] = this.backend.fmt_sqlite_mutate_query(request, env)
      //const query = _query.replaceAll('rarray(', 'carray(')
      return { query, parameters }
    } catch (e: any) {
      throw toSubzeroError(e)
    }
  }

  fmtSqliteSecondStageSelect(request: SubzeroRequest, ids: string[], env: Env): Statement {
    try {
      const [query, parameters] = this.backend.fmt_sqlite_second_stage_select(request, ids, env)
      //const query = _query.replaceAll('rarray(', 'carray(')
      return { query, parameters }
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
