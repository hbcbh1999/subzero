import { Backend } from 'subzero-wasm';
import * as fs from 'fs';
import * as path from 'path';

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
    private dbType: DbType;

    constructor(dbType: DbType, schema: any) {
        this.backend = Backend.init(JSON.stringify(schema));
        this.dbType = dbType;
    }

    get_main_query(method: Method, schemaName: string, entity: string, path: string, get: GetParameters, body: Body, headers: Headers, cookies: Cookies, env: Env): Statement {
        const [query, parameters] = this.backend.get_query(schemaName, entity, method, path, get, body ?? "", headers, cookies, env, this.dbType, false);
        return { query, parameters };
    }
    get_core_query(method: Method, schemaName: string, entity: string, path: string, get: GetParameters, body: Body, headers: Headers, ): Statement {
        const [query, parameters] = this.backend.get_query(schemaName, entity, method, path, get, body ?? "", headers, [], [], this.dbType, true);
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
export function get_introspection_query(dbType: DbType, schemas: string | string[]): Statement {
    let re = new RegExp(`{@([^#}]+)(#([^}]+))?}`, 'g');
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
            if (fs.existsSync(file_to_include)) {
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