import {Backend} from 'subzero-core-wasm';

type DbType = "postgresql" | "sqlite" | "clickhouse";
type Statement = [string, Array<string | number | boolean | null>];
type GetParameters = Array<[string, string]>;
type Method = "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
type Body = string | undefined;
type Headers = Map<string, string>;
type Cookies = Map<string, string>;

export class Subzero {
    private backend: Backend;
    private dbType: DbType;

    constructor(dbType: DbType, schema: any) {
        this.backend = Backend.init(JSON.stringify(schema));
        this.dbType = dbType;
    }

    get_main_query(method: Method, schema_name: string, entity: string, path: string, get: GetParameters, body: Body, headers: Headers, cookies: Cookies): Statement {
        const [query, parameters] = this.backend.get_query(schema_name, entity, method, path, get, body ?? "", headers, cookies, this.dbType);
        return [query, parameters];
    }
}