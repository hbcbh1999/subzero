import {Backend} from 'subzero-wasm';

type DbType = "postgresql" | "sqlite" | "clickhouse";
type Statement = [string, Array<string | number | boolean | null>];
type GetParameters = Array<[string, string]>;
type Method = "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
type Body = string | undefined;
type Headers = Array<[string, string]>;
type Cookies = Array<[string, string]>;
type Env = Array<[string, string]>;

export class Subzero {
    private backend: Backend;
    private dbType: DbType;

    constructor(dbType: DbType, schema: any) {
        this.backend = Backend.init(JSON.stringify(schema));
        this.dbType = dbType;
    }

    get_main_query(method: Method, schema_name: string, entity: string, path: string, get: GetParameters, body: Body, headers: Headers, cookies: Cookies, env: Env): Statement {
        const [query, parameters] = this.backend.get_query(schema_name, entity, method, path, get, body ?? "", headers, cookies, env, this.dbType, false);
        return [query, parameters];
    }
    get_core_query(method: Method, schema_name: string, entity: string, path: string, get: GetParameters, body: Body, headers: Headers, ): Statement {
        const [query, parameters] = this.backend.get_query(schema_name, entity, method, path, get, body ?? "", headers, [], [], this.dbType, true);
        return [query, parameters];
    }
}