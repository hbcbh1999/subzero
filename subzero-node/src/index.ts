
import * as fs from 'fs';
import * as path from 'path';
import { Backend, Request as SubzeroRequest } from 'subzero-wasm';
export { Request } from 'subzero-wasm';
export type DbType = "postgresql" | "sqlite" | "clickhouse";
export type Query = string;
export type Parameters = (string | number | boolean | null | (string|number|boolean|null)[])[];
export type Statement = { query: Query, parameters: Parameters };
export type GetParameters = ([string, string])[];
export type Method = "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
export type Body = string | undefined;
export type Headers = ([string, string])[];
export type Cookies = ([string, string])[];
export type Env = ([string, string])[];



export class Subzero {
    private backend: Backend;

    constructor(dbType: DbType, schema: any) {
        this.backend = Backend.init(JSON.stringify(schema), dbType);
    }

    async parse(schemaName: string, urlPrefix: string, role: string, request: Request): Promise<SubzeroRequest> {
        let method = request.method;
        let url = new URL(request.url);
        let path = url.pathname;
        let entity = url.pathname.substring(urlPrefix.length);
        let body = await request.text();
        // cookies are not actually used at the parse stage, they are used in the fmt stage through the env parameter
        let cookies: Cookies = [];
        let headers: Headers = [];
        request.headers.forEach((value, key) => headers.push([key, value]));
        let get: GetParameters = Array.from(url.searchParams.entries());
        return this.backend.parse(schemaName, entity, method, path, get, body, role, headers, cookies)
    }

    fmt_main_query(request: SubzeroRequest, env: Env): Statement {
        const [query, parameters] = this.backend.fmt_main_query(request, env);
        //const query = _query.replaceAll('rarray(', 'carray(');
        return { query, parameters };
    }

    fmt_sqlite_mutate_query(request: SubzeroRequest, env: Env): Statement {
        const [query, parameters] = this.backend.fmt_sqlite_mutate_query(request, env);
        //const query = _query.replaceAll('rarray(', 'carray(');
        return { query, parameters };
    }

    fmt_sqlite_second_stage_select(request: SubzeroRequest, ids: string[], env: Env): Statement {
        const [query, parameters] = this.backend.fmt_sqlite_second_stage_select(request, ids, env);
        //const query = _query.replaceAll('rarray(', 'carray(');
        return { query, parameters };
    }
    
}

export function get_raw_introspection_query(dbType: DbType): Query {
    return fs.readFileSync(path.join(__dirname, `../introspection/${dbType}_introspection_query.sql`), 'utf8');
}

/**
 * subzero allows to define custom permissions and relations in the schema through json files.
 * For that purpose, the introspection queries need a mechanism to include the contents of those files.
 * The raw queries are templates with placeholders like {@filename#default_missing_value} that are replaced by the contents of the file.
 * an example placeholder looks like {@permissions.json#[]}
 * The following function replaces the placeholders with the contents of the files if they exist.
 */
export function get_introspection_query(dbType: DbType, schemas: string | string[], placeholder_values?: Map<string, any>): Statement {
    let re = new RegExp(`{@([^#}]+)(#([^}]+))?}`, 'g');
    let placeholder_values_map = placeholder_values || new Map<string, any>();
    let raw_query = get_raw_introspection_query(dbType);
    let parts = raw_query.split(re);
    let query = '';
    for (let i = 0; i < parts.length; i += 4) {
        query += parts[i];

        if (i + 1 < parts.length) {
            let file_to_include = parts[i + 1];
            let default_value = parts[i + 3];
            if (default_value === undefined) {
                default_value = `{not found @${file_to_include}}`;
            }
            if (placeholder_values_map.has(file_to_include)) {
                query += JSON.stringify(placeholder_values_map.get(file_to_include));
            }
            else if (fs.existsSync(file_to_include)) {
                let file_content = fs.readFileSync(file_to_include, 'utf8');
                query += file_content;
            }
            else {
                query += default_value;
            }
        }
    }
    let parameters = (typeof schemas === 'string') ? [[schemas]] : [schemas];
    return { query, parameters };
}