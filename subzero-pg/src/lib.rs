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
pub enum subzero_http_method {
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
impl Display for subzero_http_method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            subzero_http_method::GET => write!(f, "GET"),
            subzero_http_method::HEAD => write!(f, "HEAD"),
            subzero_http_method::POST => write!(f, "POST"),
            subzero_http_method::PUT => write!(f, "PUT"),
            subzero_http_method::DELETE => write!(f, "DELETE"),
            subzero_http_method::CONNECT => write!(f, "CONNECT"),
            subzero_http_method::OPTIONS => write!(f, "OPTIONS"),
            subzero_http_method::TRACE => write!(f, "TRACE"),
            subzero_http_method::PATCH => write!(f, "PATCH"),
        }
    }
}

#[pg_schema]
mod subzero {
    use pgrx::lwlock::PgLwLock;
    // use pgrx::pg_sys::Hash;
    use pgrx::prelude::*;
    use pgrx::shmem::*;
    use pgrx::PgAtomic;
    use pgrx::{pg_shmem_init, warning};
    use pgrx::{GucRegistry, GucSetting, GucContext, GucFlags};
    use pgrx::Array;
    use std::ffi::CStr;
    use std::sync::atomic::Ordering;

    use std::iter::Iterator;
    // use std::path;

    use subzero_core::schema::DbSchema;
    use subzero_core::api::ApiRequest;
    use subzero_core::parser::postgrest;
    use std::borrow::Cow;
    use subzero_core::formatter::{
        Param::*,
        ToParam,
        postgresql::{generate, fmt_main_query_internal},
    };
    use subzero_core::api::{SingleVal, ListVal, Payload};
    use subzero_core::error::{*};
    use ouroboros::self_referencing;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    // use serde::{Serialize, Deserialize};

    pgrx::pg_module_magic!();

    extension_sql_file!("../sql/init.sql", requires = [subzero_http_method], name = "init",);
    extension_sql!(
        r#"
    -- introspect on extension load
    select subzero.init(
        schemas => coalesce(current_setting('subzero.db_schemas', true), 'public'),
        allow_login_roles => coalesce(current_setting('subzero.allow_login_roles', true), 'false')::boolean,
        custom_relations => current_setting('subzero.custom_relations', true),
        custom_permissions => current_setting('subzero.custom_permissions', true)
    );
    "#,
        name = "introspect",
        finalize,
    );

    const REQUEST_TYPE: &str = "subzero.http_request";
    const RESPONSE_TYPE: &str = "subzero.http_response";
    const HEADER_TYPE: &str = "subzero.http_header";

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
        //page_total: Option<i64>,
        //total_result_set: Option<i64>,
        body: String,
        constraints_satisfied: bool,
        headers: Option<String>,
        status: Option<String>,
    }

    static DB_SCHEMA: RwLock<Option<DbSchemaWrap>> = RwLock::new(None);
    static DB_SCHEMA_TIMESTAMP_LOCAL: RwLock<u64> = RwLock::new(0);

    static DB_SCHEMA_STRING: PgLwLock<heapless::Vec<u8, 1000000>> = PgLwLock::new();
    static DB_SCHEMA_TIMESTAMP: PgLwLock<u64> = PgLwLock::new();
    static UPDATING_SCHEMA: PgAtomic<std::sync::atomic::AtomicBool> = PgAtomic::new();

    lazy_static::lazy_static! {
        static ref GUC_DB_SCHEMAS: GucSetting<Option<&'static CStr>> =
                GucSetting::<Option<&'static CStr>>::new(None);
        static ref GUC_ALLOW_LOGIN_ROLES: GucSetting<bool> = GucSetting::<bool>::new(false);
        static ref GUC_CUSTOM_RELATIONS: GucSetting<Option<&'static CStr>> =
            GucSetting::<Option<&'static CStr>>::new(Some(unsafe {
                CStr::from_bytes_with_nul_unchecked(b"[]\0")
            }));
        static ref GUC_CUSTOM_PERMISSIONS: GucSetting<Option<&'static CStr>> =
            GucSetting::<Option<&'static CStr>>::new(Some(unsafe {
                CStr::from_bytes_with_nul_unchecked(b"[]\0")
            }));
    }

    #[pg_guard]
    pub extern "C" fn _PG_init() {
        pg_shmem_init!(DB_SCHEMA_STRING);
        pg_shmem_init!(DB_SCHEMA_TIMESTAMP);
        pg_shmem_init!(UPDATING_SCHEMA);

        GucRegistry::define_string_guc(
            "subzero.db_schemas",
            "The schemas to expose",
            "The schemas to expose",
            &GUC_DB_SCHEMAS,
            GucContext::Suset,
            GucFlags::default(),
        );
        GucRegistry::define_bool_guc(
            "subzero.allow_login_roles",
            "Allow login roles",
            "Allow login roles",
            &GUC_ALLOW_LOGIN_ROLES,
            GucContext::Suset,
            GucFlags::default(),
        );
        GucRegistry::define_string_guc(
            "subzero.custom_relations",
            "Custom relations",
            "Custom relations",
            &GUC_CUSTOM_RELATIONS,
            GucContext::Suset,
            GucFlags::default(),
        );
        GucRegistry::define_string_guc(
            "subzero.custom_permissions",
            "Custom permissions",
            "Custom permissions",
            &GUC_CUSTOM_PERMISSIONS,
            GucContext::Suset,
            GucFlags::default(),
        );
        // //info!("running introspection");
        // _ = Spi::run(r#"
        //     select subzero.init(
        //         schemas => coalesce(current_setting('subzero.db_schemas', true), 'public'),
        //         allow_login_roles => coalesce(current_setting('subzero.allow_login_roles', true), 'false')::boolean,
        //         custom_relations => current_setting('subzero.custom_relations', true),
        //         custom_permissions => current_setting('subzero.custom_permissions', true)
        //     );
        // "#);
        info!("subzero extension loaded");
    }

    #[pg_extern(requires = ["init"])]
    #[search_path(@extschema@)]
    pub fn init(schemas: &str, allow_login_roles: bool, custom_relations: Option<&str>, custom_permissions: Option<&str>) -> bool {
        let is_updating = UPDATING_SCHEMA.get().load(Ordering::Relaxed);
        if is_updating {
            info!("already updating schema");
            return false;
        }
        info!("init called");
        UPDATING_SCHEMA.get().store(true, Ordering::Relaxed);
        let schemas = schemas.split(',').collect::<Vec<_>>();
        let query = "select subzero.introspect_schemas($1, $2, $3, $4)";
        //info!("called with schemas: {:?}, allow_login_roles: {:?}, custom_relations: {:?}, custom_permissions: {:?}", schemas, allow_login_roles, custom_relations, custom_permissions);
        let json = Spi::connect(|client| {
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

        match json {
            Ok(Some(ss)) => {
                //info!("json schema fetched ok: {}", ss);
                let mut lock_str = DB_SCHEMA_STRING.exclusive();
                lock_str.clear();
                //lock_str.push_str(&ss);
                let r = lock_str.extend_from_slice(ss.as_bytes());
                if r.is_err() {
                    warning!("failed to save shared schema: {:?}", r);
                }
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                *DB_SCHEMA_TIMESTAMP.exclusive() = now;
                drop(lock_str);
                UPDATING_SCHEMA.get().store(false, Ordering::Relaxed);
                true
            }
            Err(e) => {
                //info!("json schema fetch failed: {}", e);
                false
            }
            _ => {
                //info!("Failed to introspect");
                false
            }
        }
    }

    #[pg_extern(requires = ["init"])]
    #[search_path(@extschema@)]
    pub fn handle(
        schema: &str, relation: &str, request: pgrx::composite_type!(REQUEST_TYPE), env: &str, max_rows: Option<i64>,
    ) -> pgrx::composite_type!('static, RESPONSE_TYPE) {
        {
            //info!("checking schema cache");
            let mut should_update_schema_cache = false;
            {
                let rl = DB_SCHEMA.read();
                let t = *DB_SCHEMA_TIMESTAMP.share();
                let tl = *DB_SCHEMA_TIMESTAMP_LOCAL.read();
                if (*rl).is_none() || t > tl {
                    should_update_schema_cache = true;
                }
            }
            if should_update_schema_cache {
                info!("updating schema cache");
                let string_vec = DB_SCHEMA_STRING.share();
                //info!("schmea vec: {:?}", string_vec.as_slice());
                let schema_string_r = String::from_utf8(string_vec.to_vec());
                if schema_string_r.is_err() {
                    //info!("failed to read shared schema");
                    let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
                    r.set_by_name("status", 500).unwrap();
                    r.set_by_name("body", "failed to read shared schema").unwrap();
                    return r;
                }
                let mut string_schema = schema_string_r.unwrap();
                if string_schema.is_empty() {
                    info!("empty schema string");
                    drop(string_vec);
                    let did_init = init(
                        GUC_DB_SCHEMAS.get().unwrap().to_str().unwrap(),
                        GUC_ALLOW_LOGIN_ROLES.get(),
                        GUC_CUSTOM_RELATIONS.get().map(|c| c.to_str().unwrap()),
                        GUC_CUSTOM_PERMISSIONS.get().map(|c| c.to_str().unwrap()),
                    );
                    if !did_init {
                        // this may be because another thread is updating, sleep for a bit
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        let string_vec = DB_SCHEMA_STRING.share();
                        //info!("schmea vec: {:?}", string_vec.as_slice());
                        let schema_string_r = String::from_utf8(string_vec.to_vec());
                        if schema_string_r.is_err() {
                            //info!("failed to read shared schema");
                            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
                            r.set_by_name("status", 500).unwrap();
                            r.set_by_name("body", "failed to read shared schema").unwrap();
                            return r;
                        }
                        string_schema = schema_string_r.unwrap();
                        //info!("got schema string after update");
                    }
                }
                //info!("schema string: {:?}", &string_schema);

                let ss = DbSchemaWrap::new(string_schema, |s| serde_json::from_str::<DbSchema>(s.as_str()).map_err(|e| e.to_string()));
                //info!("schema_wrap: {:?}", ss);
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                let mut w = DB_SCHEMA.write();
                *w = Some(ss);
                let mut ww = DB_SCHEMA_TIMESTAMP_LOCAL.write();
                *ww = now;
                //info!("local schema cache updated!!!");
            }
        }
        let s = DB_SCHEMA.read();
        if s.as_ref().is_none() {
            //info!("failed read cached schema");
            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
            r.set_by_name("status", 500).unwrap();
            r.set_by_name("body", "failed to read cached schema").unwrap();
            return r;
        }
        let db_schema_wrap = s.as_ref().unwrap();
        let db_schema = db_schema_wrap.schema();
        //info!("got schema");
        let _max_rows = max_rows.map(|m| m.to_string());
        let path: &str = request.get_by_name("path").unwrap().unwrap_or_default();
        //info!("got path {}", path);
        let _method = request.get_by_name::<crate::subzero_http_method>("method").unwrap().unwrap_or_default();
        let method = _method.to_string();
        //info!("got method {}", method);
        let query_string: &str = request.get_by_name("query_string").unwrap().unwrap_or_default();
        //info!("got query_string {}", query_string);
        let url = url::Url::parse(&format!("http://localhost/?{}", query_string))
            .unwrap_or_else(|_| url::Url::parse("http://localhost/").expect("Fallback URL should be valid"));
        let query_pairs = url.query_pairs();
        let get: Vec<(String, String)> = query_pairs.map(|(k, v)| (k.into_owned(), v.into_owned())).collect();
        let body: Option<&str> = request.get_by_name("body").unwrap();
        //info!("got body {:?}", body);
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
        //info!("got headers {:?}", headers);
        let parsed_request = postgrest::parse(
            schema,
            relation,
            db_schema,
            &method,
            path,
            get.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect(),
            body,
            headers,
            HashMap::new(),       //request.cookies,
            _max_rows.as_deref(), //None
        )
        .map_err(|e| e.to_string());
        if let Err(e) = parsed_request {
            //info!("failed to parse request: {:?}", e);
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

        let _env: HashMap<&str, &str> = serde_json::from_str(env).unwrap_or_default();
        //info!("got env");
        let statement =
            fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, &_env).map_err(|e| e.to_string());
        if let Err(e) = statement {
            //info!("failed to format statement: {:?}", e);
            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
            r.set_by_name("status", 400).unwrap();
            r.set_by_name("body", e).unwrap();
            return r;
        }
        let statement = statement.unwrap();
        let (main_statement, main_parameters, _) = generate(statement);
        //info!("main_statement: {}", main_statement);
        //info!("main_parameters: {:?}", main_parameters);
        let parameters = convert_params(main_parameters).map_err(|e| e.to_string());
        if let Err(e) = parameters {
            //info!("failed to convert parameters: {:?}", e);
            let mut r = PgHeapTuple::new_composite_type(RESPONSE_TYPE).unwrap();
            r.set_by_name("status", 400).unwrap();
            r.set_by_name("body", e).unwrap();
            return r;
        }
        let parameters = parameters.unwrap();
        let response: Result<Response, spi::SpiError> = Spi::connect(|client| {
            let row = client.select(&main_statement, None, Some(parameters))?.first();

            Ok(Response {
                //page_total: row.get::<i64>(1)?,
                //total_result_set: row.get::<i64>(2)?,
                body: row.get::<String>(3)?.unwrap_or_default(),
                constraints_satisfied: row.get::<bool>(4)?.unwrap_or(false),
                headers: row.get::<String>(5)?,
                status: row.get::<String>(6)?,
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
                //let body: &[u8] = r.body.as_bytes();
                let body = r.body.as_str();
                let mut headers_map = HashMap::from([("Content-Type", "application/json")]);
                headers_map.extend(
                    r.headers
                        .as_ref()
                        .map(|h| serde_json::from_str::<HashMap<&str, &str>>(h).unwrap_or_default())
                        .unwrap_or_default(),
                );
                //info!("RESPONSE: {:?} {:?} {:?}", status, headers_map, body);
                let headers: Vec<pgrx::composite_type!(HEADER_TYPE)> = headers_map
                    .into_iter()
                    .map(|(k, v)| {
                        let mut header = PgHeapTuple::new_composite_type(HEADER_TYPE).unwrap();
                        header.set_by_name("name", k).unwrap();
                        header.set_by_name("value", v).unwrap();
                        header
                    })
                    .collect();
                let s = http_response.set_by_name("status", status);
                let b = http_response.set_by_name("body", body);
                let h = http_response.set_by_name("headers", headers);
                //info!("RESPONSE: s:{:?} b:{:?} h:{:?}", s, b, h);
                http_response
            }
            Err(e) => {
                //info!("failed to execute query: {:?}", e);
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
        _ = Spi::run("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)");
        _ = Spi::run("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
        //info!("introspecting from test");
        _ = Spi::run(
            r#"
            select subzero.init(
                schemas => 'public'::text,
                allow_login_roles => true,
                custom_relations => '[]'::text,
                custom_permissions => '[]'::text
            )
        "#,
        );
        //info!("introspection from test succeeded");
        let request = Spi::connect(|client| {
            client
                .select(
                    r#"
                SELECT (
                    ROW(
                        'GET',                            -- method
                        '/api/data',                      -- path
                        'select=id,name&id=eq.2',         -- query_string
                        null::bytea,                      -- body
                        ARRAY[
                            ROW('Content-Type', 'application/json')::subzero.http_header,
                            ROW('Authorization', 'Bearer your_token')::subzero.http_header
                        ]                                 -- headers
                    )::subzero.http_request
                ) AS  request;
                "#,
                    None,
                    None,
                )?
                .first()
                .get_one::<pgrx::composite_type!(REQUEST_TYPE)>()
        });
        //info!("request ready");
        let request = request.unwrap().unwrap();
        let response = crate::subzero::handle("public", "users", request, "", None);

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
        assert!(crate::subzero::init("public", false, None, None));
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
        // vec![]
        vec!["shared_preload_libraries='subzero'"]
    }
}
