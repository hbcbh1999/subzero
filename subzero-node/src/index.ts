import {default as sqlite_introspection_query } from '../introspection/sqlite_introspection_query.sql';
import {default as postgresql_introspection_query } from '../introspection/postgresql_introspection_query.sql';
import {default as clickhouse_introspection_query } from '../introspection/clickhouse_introspection_query.sql';

import { default as wasmbin } from '../../subzero-wasm/pkg/subzero_wasm_bg.wasm';
import /*init, */{ initSync, Backend, Request as SubzeroRequest } from '../../subzero-wasm/pkg/subzero_wasm.js';

/* tslint:disable */
/* eslint-disable */
initSync(wasmbin);
//init(wasmbin);

export type DbType = 'postgresql' | 'sqlite' | 'clickhouse';
export type Query = string;
export type Parameters = (string | number | boolean | null | (string | number | boolean | null)[])[];
export type Statement = { query: Query; parameters: Parameters };
export type GetParameters = [string, string][];
export type Method = 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
export type Body = string | undefined;
export type Headers = [string, string][];
export type Cookies = [string, string][];
export type Env = [string, string][];
export class SubzeroError extends Error {
  message: string;
  status: number;
  description: string | null;
  constructor(msg: string, status = 500, description?: string) {
      super(msg);
      this.message = msg;
      this.status = status;
      this.description = description || null;
      // Set the prototype explicitly.
      Object.setPrototypeOf(this, SubzeroError.prototype);
  }
  statusCode():number {
      return this.status
  }
  toJSONString():string {
    let { message, description } = this;
    return JSON.stringify({ message, description });
  }
}

// export async function init_wasm() {
//   await init();
// }
function toSubzeroError(err: any) {
  let wasm_err: string = err.message;
  try {
    let ee: any = JSON.parse(wasm_err);
    return new SubzeroError(ee.message, ee.status, ee.description);
  } catch(e) {
    return new SubzeroError(wasm_err);
  }
}
export class Subzero {
  private backend: Backend;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(dbType: DbType, schema: any, allowed_select_functions?: string[]) {
    try {
      this.backend = Backend.init(JSON.stringify(schema), dbType, allowed_select_functions);
    } catch (e:any) {
      throw toSubzeroError(e);
    }
  }

  async parse(schemaName: string, urlPrefix: string, role: string, request: Request): Promise<SubzeroRequest> {
    const method = request.method;
    const url = new URL(request.url);
    const path = url.pathname;
    const entity = url.pathname.substring(urlPrefix.length);
    const body = await request.text();
    // cookies are not actually used at the parse stage, they are used in the fmt stage through the env parameter
    const cookies: Cookies = [];
    const headers: Headers = [];
    request.headers.forEach((value, key) => headers.push([key, value]));
    const get: GetParameters = Array.from(url.searchParams.entries());
    try {
      return this.backend.parse(schemaName, entity, method, path, get, body, role, headers, cookies);
    } catch (e: any) {
      throw toSubzeroError(e);
    }
  }

  fmt_main_query(request: SubzeroRequest, env: Env): Statement {
    try {
      const [query, parameters] = this.backend.fmt_main_query(request, env);
      //const query = _query.replaceAll('rarray(', 'carray(');
      return { query, parameters };
    } catch (e: any) {
      throw toSubzeroError(e);
    }
  }

  fmt_sqlite_mutate_query(request: SubzeroRequest, env: Env): Statement {
    try {
      const [query, parameters] = this.backend.fmt_sqlite_mutate_query(request, env);
      //const query = _query.replaceAll('rarray(', 'carray(');
      return { query, parameters };
    } catch (e: any) {
      throw toSubzeroError(e);
    }
  }

  fmt_sqlite_second_stage_select(request: SubzeroRequest, ids: string[], env: Env): Statement {
    try {
      const [query, parameters] = this.backend.fmt_sqlite_second_stage_select(request, ids, env);
      //const query = _query.replaceAll('rarray(', 'carray(');
      return { query, parameters };
    } catch (e: any) {
      throw toSubzeroError(e);
    }
  }
}

export function get_raw_introspection_query(dbType: DbType): Query {
  switch (dbType) {
    case 'postgresql':
      return postgresql_introspection_query;
    case 'sqlite':
      return sqlite_introspection_query;
    case 'clickhouse':
      return clickhouse_introspection_query;
    default:
      throw new Error(`Unknown dbType: ${dbType}`);
  }
  // return fs.readFileSync(path.join(__dirname, `../introspection/${dbType}_introspection_query.sql`), 'utf8');
}

/**
 * subzero allows to define custom permissions and relations in the schema through json files.
 * For that purpose, the introspection queries need a mechanism to include the contents of those files.
 * The raw queries are templates with placeholders like {@filename#default_missing_value} that are replaced by the contents of the file.
 * an example placeholder looks like {@permissions.json#[]}
 * The following function replaces the placeholders with the contents of the files if they exist.
 */
export function get_introspection_query(
  dbType: DbType,
  schemas: string | string[],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  placeholder_values?: Map<string, any>,
): Statement {
  const re = new RegExp(`{@([^#}]+)(#([^}]+))?}`, 'g');
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const placeholder_values_map = placeholder_values || new Map<string, any>();
  const raw_query = get_raw_introspection_query(dbType);
  const parts = raw_query.split(re);
  let query = '';
  for (let i = 0; i < parts.length; i += 4) {
    query += parts[i];

    if (i + 1 < parts.length) {
      const file_to_include = parts[i + 1];
      let default_value = parts[i + 3];
      if (default_value === undefined) {
        default_value = `{not found @${file_to_include}}`;
      }
      if (placeholder_values_map.has(file_to_include)) {
        query += JSON.stringify(placeholder_values_map.get(file_to_include));
      // } else if (fs.existsSync(file_to_include)) {
      //   const file_content = fs.readFileSync(file_to_include, 'utf8');
      //   query += file_content;
      } else {
        query += default_value;
      }
    }
  }
  const parameters = typeof schemas === 'string' ? [[schemas]] : [schemas];
  return { query, parameters };
}
