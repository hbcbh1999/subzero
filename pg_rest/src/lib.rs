// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
use pgrx::prelude::*;
use std::fmt::Display;

#[derive(PostgresEnum, Default)]
#[allow(non_camel_case_types)]
pub enum rest_http_method {
    #[default]
    GET,
    HEAD,
    POST,
    PUT,
    DELETE,
    CONNECT,
    OPTIONS,
    TRACE,
    PATCH,
}
impl Display for rest_http_method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            rest_http_method::GET => write!(f, "GET"),
            rest_http_method::HEAD => write!(f, "HEAD"),
            rest_http_method::POST => write!(f, "POST"),
            rest_http_method::PUT => write!(f, "PUT"),
            rest_http_method::DELETE => write!(f, "DELETE"),
            rest_http_method::CONNECT => write!(f, "CONNECT"),
            rest_http_method::OPTIONS => write!(f, "OPTIONS"),
            rest_http_method::TRACE => write!(f, "TRACE"),
            rest_http_method::PATCH => write!(f, "PATCH"),
        }
    }
}

#[pg_schema]
mod rest {
    use pgrx::prelude::*;
    use pgrx::Json;
    use once_cell::sync::Lazy;
    use pgrx::Array;
    use std::iter::Iterator;
    use subzero_core::schema::DbSchema;
    use subzero_core::api::ApiRequest;
    use subzero_core::parser::postgrest;
    use std::borrow::Cow;
    use subzero_core::formatter::{
        Param::*,
        ToParam,
        Snippet,
        SqlParam,
        postgresql::{generate, fmt_main_query_internal},
    };
    use subzero_core::dynamic_statement::{param, sql, JoinIterator};
    use subzero_core::api::{SingleVal, ListVal, Payload};
    use subzero_core::error::{*};
    use ouroboros::self_referencing;
    use std::sync::Mutex;
    use std::collections::HashMap;

    pgrx::pg_module_magic!();

    extension_sql_file!("../sql/init.sql", requires = [rest_http_method], name = "init",);

    const REQUEST_TYPE: &str = "rest.http_request";
    const RESPONSE_TYPE: &str = "rest.http_response";
    const HEADER_TYPE: &str = "rest.http_header";

    #[self_referencing]
    #[derive(Debug)]
    pub struct DbSchemaWrap {
        schema_string: String,
        #[covariant]
        #[borrows(schema_string)]
        schema: Result<DbSchema<'this>, String>,
    }
    impl DbSchemaWrap {
        pub fn schema(&self) -> &DbSchema {
            self.borrow_schema().as_ref().unwrap()
        }
    }

    #[derive(Debug)]
    pub struct Response {
        page_total: Option<i64>,
        total_result_set: Option<i64>,
        body: String,
        constraints_satisfied: bool,
        headers: Option<String>,
        status: Option<String>,
    }

    static DB_SCHEMA: Lazy<Mutex<Option<DbSchemaWrap>>> = Lazy::new(|| Mutex::new(None));

    fn fmt_env_query<'a>(env: &'a HashMap<&'a str, &'a str>) -> Snippet<'a> {
        "select "
            + if env.is_empty() {
                sql("null")
            } else {
                env.iter()
                    .map(|(k, v)| "set_config(" + param(k as &SqlParam) + ", " + param(v as &SqlParam) + ", true)")
                    .join(",")
            }
    }

    #[pg_extern(requires = ["init"])]
    #[search_path(@extschema@)]
    pub fn init(schemas: &str, allow_login_roles: bool, custom_relations: Option<&str>, custom_permissions: Option<&str>) {
        // split by comma and trim
        let schemas = schemas.split(',').map(|s| s.trim()).collect::<Vec<&str>>();
        let query = "select rest.introspect_schemas($1, $2, $3, $4)";
        debug1!(
            "init called with schemas: {:?}, allow_login_roles: {:?}, custom_relations: {:?}, custom_permissions: {:?}",
            schemas,
            allow_login_roles,
            custom_relations,
            custom_permissions
        );
        let schema_json_string = Spi::connect(|client| {
            client
                .select(
                    query,
                    None,
                    Some(vec![
                        (PgBuiltInOids::TEXTARRAYOID.oid(), schemas.into_datum()),
                        (PgBuiltInOids::BOOLOID.oid(), allow_login_roles.into_datum()),
                        (PgBuiltInOids::JSONOID.oid(), custom_permissions.into_datum()),
                        (PgBuiltInOids::JSONOID.oid(), custom_relations.into_datum()),
                    ]),
                )?
                .first()
                .get_one::<String>()
        });

        match schema_json_string {
            Ok(Some(schema_string)) => {
                debug1!("json schema fetched ok: {}", schema_string);
                let schema = DbSchemaWrap::new(schema_string, |s| serde_json::from_str::<DbSchema>(s.as_str()).map_err(|e| e.to_string()));
                let mut data = DB_SCHEMA.lock().unwrap();
                *data = Some(schema);
            }
            Err(e) => {
                error!("json schema fetch failed: {}", e);
            }
            _ => {
                error!("Failed to introspect");
            }
        }
    }

    #[pg_extern(requires = ["init"])]
    #[search_path(@extschema@)]
    pub fn handle(request: pgrx::composite_type!(REQUEST_TYPE), config: Option<Json>) -> pgrx::composite_type!('static, RESPONSE_TYPE) {
        let Json(config) = config.unwrap_or(Json(serde_json::Value::Null));
        let schema: &str = config.get("schema").and_then(|s| s.as_str()).unwrap_or("public");
        let env: HashMap<&str, &str> = config
            .get("env")
            .and_then(|s| s.as_object())
            .map(|o| o.iter().map(|(k, v)| (k.as_str(), v.as_str().unwrap_or_default())).collect())
            .unwrap_or_default();

        let max_rows = config.get("max_rows").and_then(|s| s.as_i64());
        let path_prefix: &str = config.get("path_prefix").and_then(|s| s.as_str()).unwrap_or("/");
        
        let mut should_update_schema_cache = false;
        {
            let data = DB_SCHEMA.lock().unwrap();
            if data.is_none() {
                should_update_schema_cache = true;
            }
        }
        if should_update_schema_cache {
            debug1!("updating schema cache");
            
            let schemas: &str = config.get("schemas").and_then(|s| s.as_str()).unwrap_or("public");
            let allow_login_roles = config.get("allow_login_roles").and_then(|s| s.as_bool()).unwrap_or(false);
            let custom_relations = config.get("custom_relations").and_then(|s| s.as_str());
            let custom_permissions = config.get("custom_permissions").and_then(|s| s.as_str());

            init(schemas, allow_login_roles, custom_relations, custom_permissions);
        }
        let db_schema_wrap = DB_SCHEMA.lock().unwrap();
        if db_schema_wrap.is_none() {
            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
            r.set_by_name("status", 500).unwrap();
            r.set_by_name("body", "failed to read cached schema").unwrap();
            return r;
        }
        let db_schema = db_schema_wrap.as_ref().unwrap().schema();
        let max_rows = max_rows.map(|m| m.to_string());
        let path: &str = request.get_by_name("path").unwrap().unwrap_or_default();
        debug1!("request path {}", path);
        let relation = path.strip_prefix(path_prefix).unwrap_or_default();
        debug1!("request relation {}", relation);
        let method = request
            .get_by_name::<crate::rest_http_method>("method")
            .unwrap()
            .unwrap_or_default()
            .to_string();
        debug1!("request method {}", method);
        let query_string: &str = request.get_by_name("query_string").unwrap().unwrap_or_default();
        debug1!("request query_string {}", query_string);
        let url = url::Url::parse(&format!("http://localhost/?{}", query_string))
            .unwrap_or_else(|_| url::Url::parse("http://localhost/").expect("Fallback URL should be valid"));
        let query_pairs = url.query_pairs();
        let get: Vec<(String, String)> = query_pairs.map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
        let body: Option<&str> = request.get_by_name("body").unwrap();
        debug1!("request body {:?}", body);
        let headers_array: Option<Array<pgrx::composite_type!(HEADER_TYPE)>> = request.get_by_name("headers").unwrap();
        let headers: HashMap<&str, &str> = headers_array
            .map(|a| {
                a.iter()
                    .filter_map(|h| {
                        h.map(|h| {
                            (
                                h.get_by_name("name").unwrap_or_default().unwrap_or_default(),
                                h.get_by_name("value").unwrap_or_default().unwrap_or_default(),
                            )
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        debug1!("request headers {:?}", headers);


        // try to execute set env query straight away
        let (env_query, env_parameters, _) = generate(fmt_env_query(&env));
        let r:Result<(),Error> = Spi::connect(|client| {
            debug1!("env query: {}", env_query);
            debug1!("env parameters: {:?}", env_parameters);
            let params = convert_params(env_parameters)?;
            client.select(&env_query, None, Some(params)).map_err(|e| Error::InternalError{message: e.to_string()})?;
            Ok(())
        });

        if let Err(e) = r {
            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
            r.set_by_name("status", 500).unwrap();
            r.set_by_name("body", e.to_string()).unwrap();
            return r;
        }

        let parsed_request = postgrest::parse(
            schema,
            relation,
            db_schema,
            &method,
            path,
            get.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect(),
            body,
            headers,
            HashMap::new(),      //request.cookies,
            max_rows.as_deref(), //None
        )
        .map_err(|e| e.to_string());
        if let Err(e) = parsed_request {
            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
            r.set_by_name("status", 400).unwrap();
            r.set_by_name("body", e).unwrap();
            return r;
        }
        let parsed_request = parsed_request.unwrap();

        let ApiRequest {
            method,
            schema_name,
            query,
            preferences,
            accept_content_type,
            ..
        } = parsed_request;

        //let _env: HashMap<&str, &str> = serde_json::from_str(env).unwrap_or_default();
        //info!("got env");
        let statement =
            fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, &env).map_err(|e| e.to_string());
        if let Err(e) = statement {
            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
            r.set_by_name("status", 400).unwrap();
            r.set_by_name("body", e).unwrap();
            return r;
        }
        let statement = statement.unwrap();
        let (main_statement, main_parameters, _) = generate(statement);

        debug1!("statement: {}", main_statement);
        debug1!("parameters_unparsed: {:?}", main_parameters);
        let parameters = convert_params(main_parameters).map_err(|e| e.to_string());
        if let Err(e) = parameters {
            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
            r.set_by_name("status", 400).unwrap();
            r.set_by_name("body", e).unwrap();
            return r;
        }
        let parameters = parameters.unwrap();

        
        
        debug1!("parameters: {:?}", parameters);
        let response: Result<Response, spi::SpiError> = Spi::connect(|client| {
            let row = client.select(&main_statement, None, Some(parameters))?.first();
            Ok(Response {
                page_total: row.get_by_name::<i64, _>("page_total")?,
                total_result_set: row.get_by_name::<i64, _>("total_result_set")?,
                body: row.get_by_name::<String, _>("body")?.unwrap_or_default(),
                constraints_satisfied: row.get_by_name::<bool, _>("constraints_satisfied")?.unwrap_or(false),
                headers: row.get_by_name::<String, _>("response_headers")?,
                status: row.get_by_name::<String, _>("response_status")?,
            })
        });
        match &response {
            Ok(r) => {
                if !r.constraints_satisfied {
                    let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
                    r.set_by_name("status", 400).unwrap();
                    r.set_by_name("body", "constraints not satisfied".as_bytes()).unwrap();
                    return r;
                }
                let mut http_response = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
                let status: i16 = r.status.as_ref().map(|s| s.parse::<i16>().unwrap_or(500)).unwrap_or(200);
                let body = r.body.as_str();
                let mut headers_map = HashMap::from([("Content-Type", "application/json")]);
                headers_map.extend(
                    r.headers
                        .as_ref()
                        .map(|h| serde_json::from_str::<HashMap<&str, &str>>(h).unwrap_or_default())
                        .unwrap_or_default(),
                );
                let page_total = r.page_total;
                let total_result_set = r.total_result_set;
                let headers: Vec<pgrx::composite_type!(HEADER_TYPE)> = headers_map
                    .into_iter()
                    .map(|(k, v)| {
                        let mut header = PgHeapTuple::new_composite_type(HEADER_TYPE).unwrap();
                        header.set_by_name("name", k).unwrap();
                        header.set_by_name("value", v).unwrap();
                        header
                    })
                    .collect();
                let _s = http_response.set_by_name("status", status);
                let _b = http_response.set_by_name("body", body);
                let _h = http_response.set_by_name("headers", headers);
                let _pt = http_response.set_by_name("page_total", page_total);
                let _trs = http_response.set_by_name("total_result_set", total_result_set);
                http_response
            }
            Err(e) => {
                info!("failed to execute query: {:?}", e);
                let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
                r.set_by_name("status", 500).unwrap();
                r.set_by_name("body", e.to_string()).unwrap();
                r
            }
        }
    }

    fn to_oid(dt: &str) -> pg_sys::Oid {
        match dt {
            "boolean" => bool::type_oid(),
            "integer" => i32::type_oid(),
            "bigint" => i64::type_oid(),
            "text" => pg_sys::TEXTOID,
            "json" => pg_sys::JSONOID,
            "jsonb" => pg_sys::JSONBOID,
            "uuid" => pg_sys::UUIDOID,
            "date" => pg_sys::DATEOID,
            "timestamp" => pg_sys::TIMESTAMPOID,
            "timestamptz" => pg_sys::TIMESTAMPTZOID,
            "time" => pg_sys::TIMEOID,
            "timetz" => pg_sys::TIMETZOID,
            "interval" => pg_sys::INTERVALOID,
            "numeric" | "decimal" => f64::type_oid(),
            "bytea" => pg_sys::BYTEAOID,
            "point" => pg_sys::POINTOID,
            "line" => pg_sys::LINEOID,
            "lseg" => pg_sys::LSEGOID,
            "box" => pg_sys::BOXOID,
            "path" => pg_sys::PATHOID,
            "polygon" => pg_sys::POLYGONOID,
            "circle" => pg_sys::CIRCLEOID,
            "cidr" => pg_sys::CIDROID,
            "inet" => pg_sys::INETOID,
            "macaddr" => pg_sys::MACADDROID,
            "bit" => pg_sys::BITOID,
            "varbit" => pg_sys::VARBITOID,
            "tsvector" => pg_sys::TSVECTOROID,
            "tsquery" => pg_sys::TSQUERYOID,
            "unknown" => pg_sys::UNKNOWNOID,
            _ => pg_sys::TEXTOID,
        }
    }

    fn to_datum_param(data_type: &Option<Cow<str>>, value: &str) -> Result<(PgOid, Option<pg_sys::Datum>), Error> {
        let data_type = match data_type {
            Some(t) => t,
            None => "text",
        };
        let oid = to_oid(data_type);
        let datum = match data_type {
            "boolean" => {
                let v = value.parse::<bool>().map_err(|e| Error::ParseRequestError {
                    message: String::from("failed to parse bool"),
                    details: format!("{e}"),
                })?;
                v.into_datum()
            }
            "integer" => {
                let v = value.parse::<i32>().map_err(|e| Error::ParseRequestError {
                    message: String::from("failed to parse i32"),
                    details: format!("{e}"),
                })?;
                v.into_datum()
            }
            "bigint" => {
                let v = value.parse::<i64>().map_err(|e| Error::ParseRequestError {
                    message: String::from("failed to parse i64"),
                    details: format!("{e}"),
                })?;
                v.into_datum()
            }
            "numeric" | "decimal" => {
                let v = value.parse::<f64>().map_err(|e| Error::ParseRequestError {
                    message: String::from("failed to parse numeric/decimal"),
                    details: format!("{e}"),
                })?;
                v.into_datum()
            }
            _ => {
                value.into_datum() // just send over a str
            }
        };
        Ok((oid.into(), datum))
    }

    fn convert_params(params: Vec<&(dyn ToParam + Sync)>) -> Result<Vec<(PgOid, Option<pg_sys::Datum>)>, Error> {
        params
            .iter()
            .map(|p| match p.to_param() {
                SV(SingleVal(v, d)) => to_datum_param(d, v.as_ref()),
                LV(ListVal(v, d)) => {
                    let inner_type = match d {
                        Some(t) => t.as_ref().replace("[]", ""),
                        None => String::from("text"),
                    };
                    let inner_type_oid = to_oid(inner_type.as_str());
                    let oid = unsafe { pg_sys::get_array_type(inner_type_oid) };
                    let vv = match inner_type.as_str() {
                        "boolean" => v
                            .iter()
                            .map(|i| i.as_ref().parse::<bool>())
                            .collect::<Result<Vec<bool>, _>>()
                            .map_err(|e| Error::ParseRequestError {
                                message: String::from("failed to parse bool"),
                                details: format!("{e}"),
                            })?
                            .into_datum(),
                        "integer" => v
                            .iter()
                            .map(|i| i.as_ref().parse::<i32>())
                            .collect::<Result<Vec<i32>, _>>()
                            .map_err(|e| Error::ParseRequestError {
                                message: String::from("failed to parse i32"),
                                details: format!("{e}"),
                            })?
                            .into_datum(),
                        "bigint" => v
                            .iter()
                            .map(|i| i.as_ref().parse::<i64>())
                            .collect::<Result<Vec<i64>, _>>()
                            .map_err(|e| Error::ParseRequestError {
                                message: String::from("failed to parse i64"),
                                details: format!("{e}"),
                            })?
                            .into_datum(),
                        "numeric" | "decimal" => v
                            .iter()
                            .map(|i| i.as_ref().parse::<f64>())
                            .collect::<Result<Vec<f64>, _>>()
                            .map_err(|e| Error::ParseRequestError {
                                message: String::from("failed to parse numeric/decimal"),
                                details: format!("{e}"),
                            })?
                            .into_datum(),
                        _ => v.iter().map(|i| i.as_ref()).collect::<Vec<&str>>().into_datum(),
                    };
                    Ok((oid.into(), vv))
                }
                PL(Payload(v, d)) => to_datum_param(d, v.as_ref()),
                Str(v) => to_datum_param(&Some(Cow::Borrowed("text")), v),
                StrOwned(v) => to_datum_param(&Some(Cow::Borrowed("text")), v.as_str()),
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_handle() {
        
        Spi::run("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        Spi::run("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')").unwrap();
        Spi::run("CREATE ROLE admin").unwrap();
        Spi::run("GRANT ALL ON users TO admin").unwrap();
        Spi::run("GRANT USAGE ON SCHEMA rest TO admin").unwrap();

        let request = Spi::connect(|client| {
            client
                .select(
                    r#"
                SELECT (
                    ROW(
                        'GET',                            -- method
                        '/api/users',                     -- path
                        'select=id,name&id=eq.2',         -- query_string
                        null::bytea,                      -- body
                        ARRAY[
                            ROW('Content-Type', 'application/json'),
                            ROW('Authorization', 'Bearer your_token')
                        ]::rest.http_header[]             -- headers
                    )::rest.http_request
                ) AS  request;
                "#,
                    None,
                    None,
                )?
                .first()
                .get_one::<pgrx::composite_type!(REQUEST_TYPE)>()
        });
        let request = request.unwrap().unwrap();
        info!("request ready");
        let options = serde_json::json!({
            "schema": "public",
            "env": {
                "role": "admin",
                "request.param": "1"
            },
            "path_prefix": "/api/",
            "max_rows": 100,
            "schemas": "public",
            "allow_login_roles": false,
            "custom_relations": null,
            "custom_permissions": null
        });
        Spi::run("SET log_min_messages = DEBUG1").unwrap();
        let response = crate::rest::handle(request, Some(pgrx::Json(options)));
        info!("response ready");
        let status = response.get_by_name::<i16>("status");
        let body = response.get_by_name::<&str>("body");
        let _headers = response
            .get_by_name::<Vec<pgrx::composite_type!(HEADER_TYPE)>>("headers")
            .unwrap_or_default()
            .unwrap_or_default();
        let headers = _headers
            .iter()
            .map(|h| {
                let name = h.get_by_name::<String>("name").unwrap_or_default().unwrap_or_default();
                let value = h.get_by_name::<String>("value").unwrap_or_default().unwrap_or_default();
                (name, value)
            })
            .collect::<Vec<_>>();
        assert_eq!(status, Ok(Some(200)));
        assert_eq!(body, Ok(Some(r#"[{"id":2,"name":"Bob"}]"#)));
        assert_eq!(headers, vec![("Content-Type".to_string(), "application/json".to_string())]);
    }

    #[pg_test]
    fn test_init() {
        _ = Spi::run("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)");
        _ = Spi::run("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
        //assert!(crate::subzero::init("public", false, None, None));
        let result = std::panic::catch_unwind(|| {
            crate::rest::init("public", false, None, None);
        });
        assert!(result.is_ok());
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
        //vec!["log_min_messages=DEBUG1"]
    }
}
