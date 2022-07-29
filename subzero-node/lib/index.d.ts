declare type DbType = "postgresql" | "sqlite" | "clickhouse";
declare type Statement = [string, Array<string | number | boolean | null>];
declare type GetParameters = Array<[string, string]>;
declare type Method = "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
declare type Body = string | undefined;
declare type Headers = Map<string, string>;
declare type Cookies = Map<string, string>;
export declare class Subzero {
    private backend;
    private dbType;
    constructor(dbType: DbType, schema: any);
    get_main_query(method: Method, schema_name: string, entity: string, path: string, get: GetParameters, body: Body, headers: Headers, cookies: Cookies): Statement;
}
export {};
