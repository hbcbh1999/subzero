use pgx::bgworkers::*;
use pgx::datum::{IntoDatum};
use pgx::log;
use pgx::{GucRegistry, GucSetting, GucContext};
use pgx::prelude::*;
use std::env;
use std::str::FromStr;
use std::time::Duration;
use std::convert::Infallible;
use std::fs;
use std::collections::HashMap;
use std::path::{Path};
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server};
use hyper::http::StatusCode;
use hyper::Method;
use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};
use snafu::{ResultExt, OptionExt};
use jsonpath_lib::select;
use jsonwebtoken::{decode, errors::ErrorKind, DecodingKey, Validation};
use serde_json::{from_value, Value as JsonValue};

//use hyper::u
use hyper::service::{make_service_fn, service_fn};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use subzero_core::config::{VhostConfig, SchemaStructure, SchemaStructure::*};
use subzero_core::api::DEFAULT_SAFE_SELECT_FUNCTIONS;
use subzero_core::schema::{DbSchema, include_files};
use subzero_core::formatter::{
    Param::*,
    ToParam,
    postgresql::{generate, fmt_env_query, fmt_main_query},
};
use subzero_core::error::{*};
use subzero_core::{
    api::{ApiRequest, ApiResponse, SingleVal, ListVal, Payload, QueryNode::*, ContentType::*, Preferences, Representation, Resolution},
    parser::postgrest::parse,
    permissions::{check_safe_functions, check_privileges, insert_policy_conditions},
};
use parking_lot::RwLock;
use ouroboros::self_referencing;
lazy_static::lazy_static! {
    static ref SHUTDOWN: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    static ref RESTART: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    static ref SAFE_SELECT_FUNCTIONS:String = DEFAULT_SAFE_SELECT_FUNCTIONS.join(",");
    static ref GUC_DB: GucSetting<Option<&'static str>> = GucSetting::new(None);
    static ref GUC_AUTHENTICATOR_ROLE: GucSetting<Option<&'static str>> = GucSetting::new(None);
    static ref GUC_URL_PREFIX: GucSetting<Option<&'static str>> = GucSetting::new(Some("/"));
    static ref GUC_DB_SCHEMAS: GucSetting<Option<&'static str>> = GucSetting::new(Some("public"));
    static ref GUC_DB_SCHEMA_STRUCTURE: GucSetting<Option<&'static str>> = GucSetting::new(Some(r#"{"sql_file":"introspection_query.sql"}"#));
    static ref GUC_DB_ANON_ROLE: GucSetting<Option<&'static str>> = GucSetting::new(None);
    static ref GUC_DB_MAX_ROWS: GucSetting<i32> = GucSetting::new(0);
    static ref GUC_DB_ALLOWED_SELECT_FUNCTIONS: GucSetting<Option<&'static str>> = GucSetting::new(Some(SAFE_SELECT_FUNCTIONS.as_str()));
    static ref GUC_DB_USE_LEGACY_GUCS: GucSetting<bool> = GucSetting::new(false);
    static ref GUC_DB_TX_ROLLBACK: GucSetting<bool> = GucSetting::new(false);
    static ref GUC_DB_PRE_REQUEST: GucSetting<Option<&'static str>> = GucSetting::new(None);
    static ref GUC_JWT_SECRET: GucSetting<Option<&'static str>> = GucSetting::new(None);
    static ref GUC_JWT_AUD: GucSetting<Option<&'static str>> = GucSetting::new(None);
    static ref GUC_ROLE_CLAIM_KEY: GucSetting<Option<&'static str>> = GucSetting::new(Some(".role"));
    static ref GUC_DISABLE_INTERNAL_PERMISSIONS: GucSetting<bool> = GucSetting::new(false);
    static ref GUC_LISTEN_ADDRESS: GucSetting<Option<&'static str>> = GucSetting::new(Some("localhost"));
    static ref GUC_LISTEN_PORT: GucSetting<i32> = GucSetting::new(3000);
}
static CONFIG: RwLock<Option<VhostConfig>> = RwLock::new(None);
static DB_SCHEMA: RwLock<Option<DbSchemaWrap>> = RwLock::new(None);

pgx::pg_module_magic!();

#[self_referencing]
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
fn get_current_timestamp() -> u64 {
    //TODO!!! optimize this to run once per second
    let start = SystemTime::now();
    start.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs()
}

async fn handle_request(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    // if we move the reading of body into the inner function
    // the function becomes not thread safe because of the inner await
    let (parts, body_ref) = req.into_parts();
    let method = &parts.method;
    let body_bytes = hyper::body::to_bytes(body_ref).await;
    let body = if matches!(method, &Method::GET | &Method::DELETE) {
        None
    } else {
        match body_bytes.as_ref() {
            Ok(b) => match std::str::from_utf8(b) {
                Ok(s) => Some(s),
                Err(_) => None,
            },
            Err(_) => None,
        }
    };

    match handle_request_inner(parts, body) {
        Ok(r) => Ok(r),
        Err(e) => {
            let status = StatusCode::from_u16(e.status_code()).unwrap();
            let body = e.json_body().to_string();
            Ok(Response::builder()
                .status(status)
                .header("content-type", "application/json")
                .header("server", "subzero")
                .body(Body::from(body))
                .unwrap())
        }
    }
}

fn get_env<'a>(role: Option<&'a str>, request: &'a ApiRequest, jwt_claims: &'a Option<JsonValue>) -> HashMap<Cow<'a, str>, Cow<'a, str>> {
    let mut env: HashMap<Cow<'a, str>, Cow<'a, str>> = HashMap::new();
    let search_path = &[String::from(request.schema_name)];
    if let Some(r) = role {
        env.insert("role".into(), r.into());
    }

    env.insert("request.method".into(), request.method.into());
    env.insert("request.path".into(), request.path.into());
    //pathSql = setConfigLocal mempty ("request.path", iPath req)

    env.insert("search_path".into(), search_path.join(", ").into());

    env.insert(
        "request.headers".into(),
        serde_json::to_string(&request.headers.iter().map(|(k, v)| (k.to_lowercase(), v)).collect::<Vec<_>>())
            .unwrap()
            .into(),
    );
    env.insert(
        "request.cookies".into(),
        serde_json::to_string(&request.cookies.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>())
            .unwrap()
            .into(),
    );
    env.insert(
        "request.get".into(),
        serde_json::to_string(&request.get.iter().map(|(k, v)| (k, v)).collect::<Vec<_>>())
            .unwrap()
            .into(),
    );
    match jwt_claims {
        Some(v) => {
            if let Some(claims) = v.as_object() {
                env.insert("request.jwt.claims".into(), serde_json::to_string(&claims).unwrap().into());
            }
        }
        None => {}
    }
    env
}

fn to_app_error(e: pgx::spi::Error) -> Error {
    Error::InternalError { message: e.to_string() }
}
fn content_range_header(lower: i64, upper: i64, total: Option<i64>) -> String {
    let range_string = if total != Some(0) && lower <= upper {
        format!("{lower}-{upper}")
    } else {
        "*".to_string()
    };
    let total_string = match total {
        Some(t) => format!("{t}"),
        None => "*".to_string(),
    };
    format!("{range_string}/{total_string}")
}

fn content_range_status(lower: i64, upper: i64, total: Option<i64>) -> u16 {
    match (lower, upper, total) {
        //(_, _, None) => 200,
        (l, _, Some(t)) if l > t => 406,
        (l, u, Some(t)) if (1 + u - l) < t => 206,
        _ => 200,
    }
}
fn convert_params(params: Vec<&(dyn ToParam + Sync)>) -> Vec<(PgOid, Option<pg_sys::Datum>)> {
    params
        .iter()
        .map(|p| match p.to_param() {
            SV(SingleVal(v, _)) => (PgBuiltInOids::TEXTOID.oid(), v.into_datum()),
            LV(ListVal(v, _)) => (PgBuiltInOids::TEXTARRAYOID.oid(), v.iter().map(|i| i.as_ref()).collect::<Vec<_>>().into_datum()),
            PL(Payload(v, _)) => (PgBuiltInOids::TEXTOID.oid(), v.into_datum()),
            Str(v) => (PgBuiltInOids::TEXTOID.oid(), v.into_datum()),
            StrOwned(v) => (PgBuiltInOids::TEXTOID.oid(), v.into_datum()),
        })
        .collect()
}
fn handle_request_inner(parts: hyper::http::request::Parts, body: Option<&str>) -> Result<Response<Body>, Error> {
    // read the configuration from the global variable
    log!("handle_request_inner");
    let mut response_headers = vec![];
    let c = CONFIG.read();
    let config = c.as_ref().unwrap();

    let s = DB_SCHEMA.read();
    let db_schema = s.as_ref().unwrap().schema();

    let method = parts.method;
    let uri = parts.uri;
    let path = uri.path();
    let _get = uri
        .query()
        .map(|v| url::form_urlencoded::parse(v.as_bytes()).collect())
        .unwrap_or_else(Vec::new);
    let get: Vec<(&str, &str)> = _get.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect();

    log!("handle_request_inner: get: {:?}", get);
    let headers: HashMap<&str, &str> = parts.headers.iter().map(|(k, v)| (k.as_str(), v.to_str().unwrap())).collect();

    let disable_internal_permissions = matches!(config.disable_internal_permissions, Some(true));

    let schema_name = &(match (config.db_schemas.len() > 1, &method, headers.get("accept-profile"), headers.get("content-profile")) {
        (false, ..) => Ok(config.db_schemas.get(0).unwrap().clone()),
        (_, &Method::DELETE, _, Some(content_profile))
        | (_, &Method::POST, _, Some(content_profile))
        | (_, &Method::PATCH, _, Some(content_profile))
        | (_, &Method::PUT, _, Some(content_profile)) => {
            if config.db_schemas.contains(&String::from(*content_profile)) {
                Ok(content_profile.to_string())
            } else {
                Err(Error::UnacceptableSchema {
                    schemas: config.db_schemas.clone(),
                })
            }
        }
        (_, _, Some(accept_profile), _) => {
            if config.db_schemas.contains(&String::from(*accept_profile)) {
                Ok(accept_profile.to_string())
            } else {
                Err(Error::UnacceptableSchema {
                    schemas: config.db_schemas.clone(),
                })
            }
        }
        _ => Ok(config.db_schemas.get(0).unwrap().clone()),
    })?;

    if config.db_schemas.len() > 1 {
        response_headers.push(("Content-Profile".to_string(), schema_name.clone()));
    }
    let jwt_claims = match &config.jwt_secret {
        Some(key) => match headers.get("authorization") {
            Some(a) => {
                let token_str: Vec<&str> = a.split(' ').collect();
                match token_str[..] {
                    ["Bearer", t] | ["bearer", t] => {
                        let mut validation = Validation::default();
                        validation.validate_exp = false;
                        validation.set_required_spec_claims::<&str>(&[]);
                        match decode::<JsonValue>(t, &DecodingKey::from_secret(key.as_bytes()), &validation) {
                            Ok(c) => {
                                if let Some(exp) = c.claims.get("exp") {
                                    if from_value::<u64>(exp.clone()).context(JsonSerializeSnafu)? < get_current_timestamp() - 1 {
                                        return Err(Error::JwtTokenInvalid {
                                            message: "JWT expired".to_string(),
                                        });
                                    }
                                }
                                Ok(Some(c.claims))
                            }
                            Err(err) => match *err.kind() {
                                ErrorKind::InvalidToken => Err(Error::JwtTokenInvalid { message: format!("{err}") }),
                                _ => Err(Error::JwtTokenInvalid { message: format!("{err}") }),
                            },
                        }
                    }
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        },
        None => Ok(None),
    }?;

    let (role, authenticated) = match &jwt_claims {
        Some(claims) => match select(claims, format!("${}", config.role_claim_key).as_str()) {
            Ok(v) => match &v[..] {
                [JsonValue::String(s)] => Ok((Some(s), true)),
                _ => Ok((config.db_anon_role.as_ref(), true)),
            },
            Err(e) => Err(Error::JwtTokenInvalid { message: format!("{e}") }),
        },
        None => Ok((config.db_anon_role.as_ref(), false)),
    }?;

    // do not allow unauthenticated requests when there is no anonymous role setup
    if let (None, false) = (role, authenticated) {
        return Err(Error::JwtTokenInvalid {
            message: "unauthenticated requests not allowed".to_string(),
        });
    }

    //TODO!!!: eliminate the following 3 iterations
    let max_rows = config.db_max_rows.iter().map(|m| m.to_string()).next();
    let max_rows = max_rows.iter().map(|m| m.as_str()).next();
    let db_allowed_select_functions = config.db_allowed_select_functions.iter().map(|m| m.as_str()).collect::<Vec<_>>();
    let role = match role {
        Some(r) => r,
        None => "",
    };
    let slash = "/".to_string(); //TODO: make this a constant
    let prefix = config.url_prefix.as_ref().unwrap_or(&slash);
    let table = path.replace(prefix.as_str(), "");

    let cookies = HashMap::from([]);
    log!("handle_request_inner: table: {}", table);
    // parse request and generate the query
    let mut request = parse(schema_name, &table, db_schema, method.as_str(), path, get, body, headers, cookies, max_rows)?;
    log!("request parsed");
    // in case when the role is not set (but authenticated through jwt) the query will be executed with the privileges
    // of the "authenticator" role unless the DbSchema has internal privileges set
    if !disable_internal_permissions {
        check_privileges(db_schema, schema_name, role, &request)?;
    }
    check_safe_functions(&request, &db_allowed_select_functions)?;
    if !disable_internal_permissions {
        insert_policy_conditions(db_schema, schema_name, role, &mut request.query)?;
    }
    // when using internal privileges not switch "current_role"
    let env_role = if !disable_internal_permissions && db_schema.use_internal_permissions {
        None
    } else {
        Some(role)
    };

    let _env = get_env(env_role, &request, &jwt_claims);
    let env = _env.iter().map(|(k, v)| (k.as_ref(), v.as_ref())).collect::<HashMap<_, _>>();
    log!("env: {:?}", env);
    // now that we have request and env we can generate the query and execute it
    let (env_query, env_parameters, _) = generate(fmt_env_query(&env));
    let (main_statement, main_parameters, _) = generate(fmt_main_query(db_schema, schema_name, &request, &env)?);
    log!("main_statement: {}", main_statement);
    log!("main_parameters: {:?}", main_parameters);
    let env_parameters = convert_params(env_parameters);
    let main_parameters = convert_params(main_parameters);
    log!("parameters converted : {:?}", main_parameters);
    let response = BackgroundWorker::transaction(|| {
        Spi::connect(|mut c| -> Result<ApiResponse, Error> {
            c.select(&env_query, None, Some(env_parameters)).map_err(to_app_error)?;
            log!("env_query executed");
            let row = c.update(&main_statement, None, Some(main_parameters)).map_err(to_app_error)?;
            log!("main_statement executed {:?}", row);
            let row = row.first();
            log!("main_statement executed first {:?}", row);
            let constraints_satisfied = row
                .get_by_name::<bool, _>("constraints_satisfied")
                .map_err(to_app_error)?
                .context(InternalSnafu {
                    message: "could not read constraints_satisfied colum".to_string(),
                })?;
            if !constraints_satisfied {
                //transaction.rollback().await.context(PgDbSnafu { authenticated })?;
                return Err(Error::PermissionDenied {
                    details: "check constraint of an insert/update permission has failed".to_string(),
                });
            }
            let page_total = row.get_by_name::<i64, _>("page_total").map_err(to_app_error)?.context(InternalSnafu {
                message: "could not read page_total colum".to_string(),
            })?;
            let total_result_set = row.get_by_name::<i64, _>("total_result_set").map_err(to_app_error)?.map(|v| v as u64);
            let response_headers = row.get_by_name::<String, _>("response_headers").map_err(to_app_error)?;
            let response_status = row.get_by_name::<String, _>("response_status").map_err(to_app_error)?;
            let body = row.get_by_name::<String, _>("body").map_err(to_app_error)?.context(InternalSnafu {
                message: "could not read body colum".to_string(),
            })?;
            log!("db response read");
            Ok(ApiResponse {
                page_total: page_total as u64,
                total_result_set,
                top_level_offset: 0,
                response_headers,
                response_status,
                body,
            })
        })
    })?;

    // now that we have ApiResponse se calculate things needed for the HTTP response
    // create and return the response to the client
    let page_total = response.page_total;
    let total_result_set = response.total_result_set;
    let top_level_offset = response.top_level_offset;
    let response_content_type = match (&request.accept_content_type, &request.query.node) {
        (SingularJSON, _)
        | (
            _,
            FunctionCall {
                returns_single: true,
                is_scalar: false,
                ..
            },
        ) => SingularJSON,
        (TextCSV, _) => TextCSV,
        _ => ApplicationJSON,
    };

    if let Some(response_headers_str) = response.response_headers {
        match serde_json::from_str(response_headers_str.as_str()) {
            Ok(JsonValue::Array(headers_json)) => {
                for h in headers_json {
                    match h {
                        JsonValue::Object(o) => {
                            for (k, v) in o.into_iter() {
                                match v {
                                    JsonValue::String(s) => {
                                        response_headers.push((k, s));
                                        Ok(())
                                    }
                                    _ => Err(Error::GucHeadersError),
                                }?
                            }
                            Ok(())
                        }
                        _ => Err(Error::GucHeadersError),
                    }?
                }
                Ok(())
            }
            _ => Err(Error::GucHeadersError),
        }?
    }

    let lower = top_level_offset as i64;
    let upper = top_level_offset as i64 + page_total as i64 - 1;
    let total = total_result_set.map(|t| t as i64);
    let content_range = match (&method, &request.query.node) {
        (&Method::POST, Insert { .. }) => content_range_header(1, 0, total),
        (&Method::DELETE, Delete { .. }) => content_range_header(1, upper, total),
        _ => content_range_header(lower, upper, total),
    };
    response_headers.push(("Content-Range".to_string(), content_range));

    #[rustfmt::skip]
    let mut status = match (&method, &request.query.node, page_total, &request.preferences) {
        (&Method::POST,   Insert { .. }, ..) => 201,
        (&Method::DELETE, Delete { .. }, _, Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::DELETE, Delete { .. }, ..) => 204,
        (&Method::PATCH,  Update { columns, .. }, 0, _) if !columns.is_empty() => 404,
        (&Method::PATCH,  Update { .. }, _,Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::PATCH,  Update { .. }, ..) => 204,
        (&Method::PUT,    Insert { .. },_,Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::PUT,    Insert { .. }, ..) => 204,
        _ => content_range_status(lower, upper, total),
    };

    if let Some(Preferences { resolution: Some(r), .. }) = request.preferences {
        response_headers.push((
            "Preference-Applied".to_string(),
            match r {
                Resolution::MergeDuplicates => "resolution=merge-duplicates".to_string(),
                Resolution::IgnoreDuplicates => "resolution=ignore-duplicates".to_string(),
            },
        ));
    }

    let response_status: Option<String> = response.response_status;
    if let Some(response_status_str) = response_status {
        status = response_status_str.parse::<u16>().map_err(|_| Error::GucStatusError)?;
    }

    log!("start http response build");
    //log!("hello world called {:?}", config);
    //let body = format!("{main_statement} {main_parameters:?}");
    //let body = "hello world";
    //Ok((status, content_type, response_headers, response.body))
    let mut http_response = Response::builder().status(status);
    let headers = http_response.headers_mut().unwrap();
    let http_content_type = match response_content_type {
        SingularJSON => Ok("application/vnd.pgrst.object+json"),
        TextCSV => Ok("text/csv"),
        ApplicationJSON => Ok("application/json"),
        Other(t) => Err(Error::ContentTypeError {
            message: format!("None of these Content-Types are available: {t}"),
        }),
    }?;
    headers.insert("content-type", http_content_type.parse().unwrap());
    headers.insert("server", "subzero".parse().unwrap());
    for (k, v) in response_headers {
        let name = hyper::header::HeaderName::from_str(k.as_str()).map_err(|_| Error::InternalError {
            message: "could not create hyper header".to_string(),
        })?;
        let value = hyper::header::HeaderValue::from_str(v.as_str()).map_err(|_| Error::InternalError {
            message: "could not create hyper header".to_string(),
        })?;
        headers.insert(name, value);
    }
    match http_response.body(response.body.into()) {
        Ok(r) => Ok(r),
        Err(_) => Err(Error::InternalError {
            message: "could not create hyper response".to_string(),
        }),
    }
}

async fn shutdown_restart_signal() {
    // Wait for the CTRL+C signal
    while !(SHUTDOWN.load(Ordering::SeqCst) || RESTART.load(Ordering::SeqCst)) {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if BackgroundWorker::sigterm_received() {
            SHUTDOWN.store(true, Ordering::SeqCst);
        }
        if BackgroundWorker::sighup_received() {
            RESTART.store(true, Ordering::SeqCst);
        }
    }
}

fn load_configuration() -> Result<(), String> {
    log!("reading config");
    let db_schemas = GUC_DB_SCHEMAS.get().map(|s| s.split(',').map(|s| s.to_string()).collect::<Vec<_>>());
    let db_schemas = match db_schemas {
        Some(s) => Ok(s),
        None => Err("subzero.db_schemas is required".to_string()),
    }?;
    let db_max_rows = match GUC_DB_MAX_ROWS.get() {
        0 => None,
        n => Some(n as u32),
    };
    let db_allowed_select_functions = GUC_DB_ALLOWED_SELECT_FUNCTIONS
        .get()
        .map(|s| s.split(',').map(|s| s.to_string()).collect::<Vec<_>>());
    let db_allowed_select_functions = match db_allowed_select_functions {
        Some(s) => Ok(s),
        None => Err("subzero.db_allowed_select_functions is required".to_string()),
    }?;
    let db_pre_request = GUC_DB_PRE_REQUEST
        .get()
        .and_then(|s| s.split_once('.').map(|(s, f)| (s.to_string(), f.to_string())));
    let db_schema_structure = GUC_DB_SCHEMA_STRUCTURE
        .get()
        .map(|s| serde_json::from_str::<SchemaStructure>(&s).map_err(|e| e.to_string()));
    let db_schema_structure = match db_schema_structure {
        Some(s) => Ok(s),
        None => Err("subzero.db_schema_structure is required".to_string()),
    }??;
    // check the file specified in the schema structure exists
    match &db_schema_structure {
        SqlFile(f) | JsonFile(f) => {
            let path = Path::new(&f);
            match path.try_exists() {
                Ok(true) => Ok(()),
                Ok(false) => {
                    let p = if path.is_absolute() {
                        path.display().to_string()
                    } else {
                        format!("{} (relative to current directory {})", path.display(), env::current_dir().unwrap().display())
                    };
                    Err(format!("subzero.db_schema_structure file does not exist: {p}"))
                }
                Err(e) => Err(format!("subzero.db_schema_structure can't check existence of file (check permissions): {e}")),
            }
        }
        JsonString(s) => {
            let r = serde_json::from_str::<serde_json::Value>(s).map_err(|e| e.to_string());
            match r {
                Ok(_) => Ok(()),
                Err(e) => Err(format!("subzero.db_schema_structure json is invalid: {e}")),
            }
        }
    }?;

    // execute the introspection query and set DB_SCHEMA
    let db_schema: DbSchemaWrap = match db_schema_structure.clone() {
        SqlFile(f) => match fs::read_to_string(f) {
            Ok(q) => {
                let query = include_files(q);
                let s = BackgroundWorker::transaction(|| {
                    Spi::connect(|client| {
                        client
                            .select(&query, None, Some(vec![(PgBuiltInOids::TEXTARRAYOID.oid(), db_schemas.clone().into_datum())]))?
                            .first()
                            .get_one::<String>()
                    })
                })
                .map_err(|e| e.to_string())?;
                let ss = match s {
                    Some(s) => Ok(s),
                    None => Err("subzero.db_schema_structure query returned no rows/columns".to_string()),
                }?;

                Ok(DbSchemaWrap::new(ss, |s| serde_json::from_str::<DbSchema>(s.as_str()).map_err(|e| e.to_string())))
                //let _ = transaction.query("set local schema ''", &[]).await;
            }
            Err(e) => Err(format!("{e}")),
        },
        JsonFile(f) => match fs::read_to_string(f) {
            Ok(s) => Ok(DbSchemaWrap::new(s, |s| serde_json::from_str::<DbSchema>(s).map_err(|e| e.to_string()))),
            Err(e) => Err(format!("{e}")),
        },
        JsonString(s) => Ok(DbSchemaWrap::new(s, |s| serde_json::from_str::<DbSchema>(s.as_str()).map_err(|e| e.to_string()))),
    }?;

    let config = VhostConfig {
        db_type: "postgres".to_string(),
        static_files_dir: None,
        db_uri: String::new(),
        db_pool: 0,
        url_prefix: GUC_URL_PREFIX.get(),
        db_schemas,
        db_anon_role: GUC_DB_ANON_ROLE.get(),
        db_max_rows,
        db_allowed_select_functions,
        db_use_legacy_gucs: GUC_DB_USE_LEGACY_GUCS.get(),
        db_tx_rollback: GUC_DB_TX_ROLLBACK.get(),
        db_pre_request,
        jwt_secret: GUC_JWT_SECRET.get(),
        jwt_aud: GUC_JWT_AUD.get(),
        role_claim_key: GUC_ROLE_CLAIM_KEY.get().unwrap_or_else(|| ".role".to_string()),
        disable_internal_permissions: Some(GUC_DISABLE_INTERNAL_PERMISSIONS.get()),
        db_schema_structure,
    };
    *CONFIG.write() = Some(config);
    *DB_SCHEMA.write() = Some(db_schema);
    let s = DB_SCHEMA.read();
    let ss = s.as_ref().unwrap().borrow_schema().as_ref().unwrap();
    log!("db schema loaded {:?}", ss);
    log!("config loaded {:?}", CONFIG.read().as_ref().unwrap());
    Ok(())
}

async fn start_webserver() {
    // We'll bind to 127.0.0.1:3000
    let address = GUC_LISTEN_ADDRESS.get();
    if address.is_none() {
        log!("subzero.listen_address is required");
        return;
    }
    let port = GUC_LISTEN_PORT.get();
    if port == 0 {
        log!("subzero.listen_port is required");
        return;
    }
    let addr = SocketAddr::from_str(&format!("{}:{}", address.unwrap(), port));
    if let Err(e) = addr {
        log!("subzero.listen_address or subzero.listen_port is invalid: {}", e);
        return;
    }
    let addr = addr.unwrap();

    while !SHUTDOWN.load(Ordering::SeqCst) {
        RESTART.store(false, Ordering::SeqCst);
        if let Err(e) = load_configuration() {
            log!("failed to load configuration: {}", e);
            break;
        }
        log!("starting webserver");

        let make_svc = make_service_fn(|_conn| async move { Ok::<_, Infallible>(service_fn(handle_request)) });
        let server = Server::bind(&addr).serve(make_svc).with_graceful_shutdown(shutdown_restart_signal());

        // Run this server for... forever!
        if let Err(e) = server.await {
            log!("server error: {}", e);
        }
        log!("stopping webserver");
    }
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    GucRegistry::define_string_guc(
        "subzero.database",
        "The database for which to enable subzero",
        "The database for which to enable subzero",
        &GUC_DB,
        GucContext::Suset,
    );

    GucRegistry::define_string_guc(
        "subzero.authenticator_role",
        "The databse role used for executing queries",
        "The databse role used for executing queries before swithcing to the user's role",
        &GUC_AUTHENTICATOR_ROLE,
        GucContext::Suset,
    );

    GucRegistry::define_string_guc(
        "subzero.url_prefix",
        "The URL prefix for subzero",
        "The URL prefix for subzero",
        &GUC_URL_PREFIX,
        GucContext::Suset,
    );

    GucRegistry::define_string_guc("subzero.db_schemas", "The schemas to expose", "The schemas to expose", &GUC_DB_SCHEMAS, GucContext::Suset);

    GucRegistry::define_string_guc(
        "subzero.db_anon_role",
        "The role to use for anonymous requests",
        "The role to use for anonymous requests",
        &GUC_DB_ANON_ROLE,
        GucContext::Suset,
    );

    GucRegistry::define_int_guc(
        "subzero.db_max_rows",
        "The maximum number of rows to return",
        "The maximum number of rows to return",
        &GUC_DB_MAX_ROWS,
        0,
        i32::MAX,
        GucContext::Suset,
    );

    GucRegistry::define_string_guc(
        "subzero.db_allowed_select_functions",
        "The functions that can be called with the select verb",
        "The functions that can be called with the select verb",
        &GUC_DB_ALLOWED_SELECT_FUNCTIONS,
        GucContext::Suset,
    );

    GucRegistry::define_bool_guc("subzero.db_use_legacy_gucs", "Use legacy gucs", "Use legacy gucs", &GUC_DB_USE_LEGACY_GUCS, GucContext::Suset);

    GucRegistry::define_bool_guc("subzero.db_tx_rollback", "Rollback transactions", "Rollback transactions", &GUC_DB_TX_ROLLBACK, GucContext::Suset);

    GucRegistry::define_string_guc(
        "subzero.db_pre_request",
        "SQL function to execute before each request",
        "SQL function to execute before each request",
        &GUC_DB_PRE_REQUEST,
        GucContext::Suset,
    );

    GucRegistry::define_string_guc("subzero.jwt_secret", "Jwt secret", "Jwt secret", &GUC_JWT_SECRET, GucContext::Suset);

    GucRegistry::define_string_guc("subzero.jwt_aud", "Jwt aud", "Jwt aud", &GUC_JWT_AUD, GucContext::Suset);

    GucRegistry::define_string_guc("subzero.role_claim_key", "Role claim key", "Role claim key", &GUC_ROLE_CLAIM_KEY, GucContext::Suset);

    GucRegistry::define_bool_guc(
        "subzero.disable_internal_permissions",
        "Disable internal permissions",
        "Disable internal permissions",
        &GUC_DISABLE_INTERNAL_PERMISSIONS,
        GucContext::Suset,
    );

    GucRegistry::define_string_guc(
        "subzero.db_schema_structure",
        "Disable internal permissions",
        "Disable internal permissions",
        &GUC_DB_SCHEMA_STRUCTURE,
        GucContext::Suset,
    );

    GucRegistry::define_string_guc(
        "subzero.listen_addresses",
        "Listen addresses",
        "Listen addresses",
        &GUC_LISTEN_ADDRESS,
        GucContext::Suset,
    );

    GucRegistry::define_int_guc(
        "subzero.port",
        "Port",
        "Port",
        &GUC_LISTEN_PORT,
        0,
        i32::MAX,
        GucContext::Suset,
    );

    log!("subzero gucs registered");

    BackgroundWorkerBuilder::new("subzero_pgx")
        .set_function("background_worker_main")
        .set_library("subzero_pgx")
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_main() {

    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    log!("Background Worker '{}' is starting.", BackgroundWorker::get_name());

    let db = GUC_DB.get();
    if db.is_none() {
        log!("subzero.database not set, exiting");
        return;
    }
    if let Some(role) = GUC_AUTHENTICATOR_ROLE.get() {
        BackgroundWorker::connect_worker_to_spi(Some(db.unwrap().as_str()), Some(role.as_str()));
    } else {
        BackgroundWorker::connect_worker_to_spi(Some(db.unwrap().as_str()), None);
    }

    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

    rt.block_on(start_webserver());

    log!("Background Worker '{}' is exiting", BackgroundWorker::get_name());
}
