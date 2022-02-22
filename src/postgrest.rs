#[cfg(feature = "postgresql")]
use deadpool_postgres::Pool;
use http::Method;
use jsonpath_lib::select;
use jsonwebtoken::{decode, errors::ErrorKind, DecodingKey, Validation};
use serde_json::{from_value, Value as JsonValue};
use snafu::ResultExt;
#[cfg(feature = "postgresql")]
use subzero::dynamic_statement::{param, JoinIterator, SqlSnippet};
#[cfg(feature = "postgresql")]
use subzero::formatter::postgresql::fmt_main_query;
#[cfg(feature = "postgresql")]
use tokio_postgres::{types::ToSql, IsolationLevel};

#[cfg(feature = "sqlite")]
use r2d2::Pool;
#[cfg(feature = "sqlite")]
use r2d2_sqlite::SqliteConnectionManager;
#[cfg(feature = "sqlite")]
use rusqlite::params_from_iter;
#[cfg(feature = "sqlite")]
use subzero::formatter::sqlite::fmt_main_query;
#[cfg(feature = "sqlite")]
use tokio::task;

use subzero::{
    api::{
        ApiRequest, ApiResponse, ContentType, ContentType::*, Preferences, QueryNode::*,
        Representation, Resolution::*,
    },
    config::VhostConfig,
    dynamic_statement::generate,
    error::{Result, *},
    parser::postgrest::parse,
    schema::DbSchema,
};

use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "postgresql")]
fn get_postgrest_env(
    role: &String,
    search_path: &Vec<String>,
    request: &ApiRequest,
    jwt_claims: &Option<JsonValue>,
) -> HashMap<String, String> {
    let mut env = HashMap::new();
    env.insert("role".to_string(), role.clone());
    env.insert("request.method".to_string(), format!("{}", request.method));
    env.insert("request.path".to_string(), format!("{}", request.path));
    //pathSql = setConfigLocal mempty ("request.path", iPath req)
    env.insert("request.jwt.claim.role".to_string(), role.clone());
    env.insert(
        "search_path".to_string(),
        search_path.join(", ").to_string(),
    );
    env.extend(request.headers.iter().map(|(k, v)| {
        (
            format!("request.header.{}", k.to_lowercase()),
            v.to_string(),
        )
    }));
    env.extend(
        request
            .cookies
            .iter()
            .map(|(k, v)| (format!("request.cookie.{}", k), v.to_string())),
    );
    match jwt_claims {
        Some(v) => match v.as_object() {
            Some(claims) => {
                env.extend(claims.iter().map(|(k, v)| {
                    (
                        format!("request.jwt.claim.{}", k),
                        match v {
                            JsonValue::String(s) => s.clone(),
                            _ => format!("{}", v),
                        },
                    )
                }));
            }
            None => {}
        },
        None => {}
    }
    env
}

#[cfg(feature = "postgresql")]
fn get_postgrest_env_query<'a>(
    env: &'a HashMap<String, String>,
) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)> {
    "select "
        + env
            .iter()
            .map(|(k, v)| {
                "set_config("
                    + param(k as &(dyn ToSql + Sync + 'a))
                    + ", "
                    + param(v as &(dyn ToSql + Sync + 'a))
                    + ", true)"
            })
            .join(",")
}

fn get_current_timestamp() -> u64 {
    //TODO!!! optimize this to run once per second
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

#[cfg(feature = "postgresql")]
async fn query_postgresql<'a>(
    method: &Method,
    pool: &'a Pool,
    readonly: bool,
    authenticated: bool,
    schema_name: &String,
    request: &ApiRequest<'_>,
    role: &String,
    jwt_claims: &Option<JsonValue>,
    config: &VhostConfig,
) -> Result<ApiResponse> {
    let mut client = pool.get().await.context(DbPoolError)?;

    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .read_only(readonly)
        .start()
        .await
        .context(DbError { authenticated })?;
    let (main_statement, main_parameters, _) = generate(fmt_main_query(schema_name, request)?);
    // println!(
    //     "main_statement: \n{}\n{:?}",
    //     main_statement, main_parameters
    // );
    let env = get_postgrest_env(role, &vec![schema_name.clone()], request, jwt_claims);
    let (env_statement, env_parameters, _) = generate(get_postgrest_env_query(&env));

    //TODO!!! optimize this so we run both queries in paralel
    let env_stm = transaction
        .prepare_cached(env_statement.as_str())
        .await
        .context(DbError { authenticated })?;
    let _ = transaction
        .query(&env_stm, env_parameters.as_slice())
        .await
        .context(DbError { authenticated })?;

    if let Some((s, f)) = &config.db_pre_request {
        let fn_schema = match s.as_str() {
            "" => schema_name,
            _ => &s,
        };

        let pre_request_statement = format!(r#"select "{}"."{}"()"#, fn_schema, f);
        let pre_request_stm = transaction
            .prepare_cached(pre_request_statement.as_str())
            .await
            .context(DbError { authenticated })?;
        transaction
            .query(&pre_request_stm, &[])
            .await
            .context(DbError { authenticated })?;
    }

    let main_stm = transaction
        .prepare_cached(main_statement.as_str())
        .await
        .context(DbError { authenticated })?;

    let rows = transaction
        .query(&main_stm, main_parameters.as_slice())
        .await
        .context(DbError { authenticated })?;

    // let (env_stm, main_stm) = future::try_join(
    //         transaction.prepare_cached(env_statement.as_str()),
    //         transaction.prepare_cached(main_statement.as_str())
    //     ).await.context(DbError)?;

    // let (_, rows) = future::try_join(
    //     transaction.query(&env_stm, env_parameters.as_slice()),
    //     transaction.query(&main_stm, main_parameters.as_slice())
    // ).await.context(DbError)?;
    let api_response = ApiResponse {
        page_total: rows[0].get("page_total"),
        total_result_set: rows[0].get("total_result_set"),
        top_level_offset: 0,
        response_headers: rows[0].get("response_headers"),
        response_status: rows[0].get("response_status"),
        body: rows[0].get("body"),
    };

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        transaction
            .rollback()
            .await
            .context(DbError { authenticated })?;
        return Err(Error::SingularityError {
            count: api_response.page_total,
            content_type: "application/vnd.pgrst.object+json".to_string(),
        });
    }

    if method == Method::PUT && api_response.page_total != 1 {
        // Makes sure the querystring pk matches the payload pk
        // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
        // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
        // If this condition is not satisfied then nothing is inserted,
        transaction
            .rollback()
            .await
            .context(DbError { authenticated })?;
        return Err(Error::PutMatchingPkError);
    }

    if config.db_tx_rollback {
        transaction
            .rollback()
            .await
            .context(DbError { authenticated })?;
    } else {
        transaction
            .commit()
            .await
            .context(DbError { authenticated })?;
    }

    Ok(api_response)
}

#[cfg(feature = "sqlite")]
fn query_sqlite(
    method: &Method,
    pool: &Pool<SqliteConnectionManager>,
    _readonly: bool,
    authenticated: bool,
    schema_name: &String,
    request: &ApiRequest<'_>,
    _role: &String,
    _jwt_claims: &Option<JsonValue>,
    config: &VhostConfig,
) -> Result<ApiResponse> {
    let conn = pool.get().unwrap();

    conn.execute_batch("BEGIN DEFERRED")
        .context(DbError { authenticated })?;
    //let transaction = conn.transaction().context(DbError { authenticated })?;

    let (main_statement, main_parameters, _) = generate(fmt_main_query(schema_name, request)?);
    println!(
        "main_statement: {} \n{}",
        main_parameters.len(),
        main_statement
    );
    // for p in params_from_iter(main_parameters.iter()) {
    //     println!("p {:?}", p.to_sql());
    // }
    for p in main_parameters.iter() {
        println!("p {:?}", p.to_sql());
    }
    let mut main_stm = conn
        .prepare_cached(main_statement.as_str())
        .map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            e
        })
        .context(DbError { authenticated })?;

    let mut rows = main_stm
        .query(params_from_iter(main_parameters.iter()))
        .map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            e
        })
        .context(DbError { authenticated })?;

    let main_row = rows.next().context(DbError { authenticated })?.unwrap();
    let api_response = ApiResponse {
        page_total: main_row.get(0).context(DbError { authenticated })?, //("page_total"),
        total_result_set: main_row.get(1).context(DbError { authenticated })?, //("total_result_set"),
        top_level_offset: 0,
        body: main_row.get(2).context(DbError { authenticated })?, //("body"),
        response_headers: main_row.get(3).context(DbError { authenticated })?, //("response_headers"),
        response_status: main_row.get(4).context(DbError { authenticated })?, //("response_status"),
    };

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        conn.execute_batch("ROLLBACK")
            .context(DbError { authenticated })?;
        return Err(Error::SingularityError {
            count: api_response.page_total,
            content_type: "application/vnd.pgrst.object+json".to_string(),
        });
    }

    //println!("before check {:?} {:?}", method, page_total);
    if method == &Method::PUT && api_response.page_total != 1 {
        // Makes sure the querystring pk matches the payload pk
        // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
        // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
        // If this condition is not satisfied then nothing is inserted,
        conn.execute_batch("ROLLBACK")
            .context(DbError { authenticated })?;
        return Err(Error::PutMatchingPkError);
    }

    if config.db_tx_rollback {
        conn.execute_batch("ROLLBACK")
            .context(DbError { authenticated })?;
    } else {
        conn.execute_batch("COMMIT")
            .context(DbError { authenticated })?;
    }

    Ok(api_response)
}

pub async fn handle_postgrest_request(
    config: &VhostConfig,
    root: &String,
    method: &Method,
    path: String,
    parameters: &Vec<(&str, &str)>,
    db_schema: &DbSchema,
    #[cfg(feature = "postgresql")] pool: &Pool,
    #[cfg(feature = "sqlite")] pool: &Pool<SqliteConnectionManager>,
    #[cfg(feature = "clickhouse")] pool: &Option<String>,
    body: Option<String>,
    headers: &HashMap<&str, &str>,
    cookies: &HashMap<&str, &str>,
) -> Result<(u16, ContentType, Vec<(String, String)>, String)> {
    let mut response_headers = vec![];
    let schema_name = &(match (
        config.db_schemas.len() > 1,
        method,
        headers.get("Accept-Profile"),
        headers.get("Content-Profile"),
    ) {
        (false, ..) => Ok(config.db_schemas.get(0).unwrap().clone()),
        (_, &Method::DELETE, _, Some(&content_profile))
        | (_, &Method::POST, _, Some(&content_profile))
        | (_, &Method::PATCH, _, Some(&content_profile))
        | (_, &Method::PUT, _, Some(&content_profile)) => {
            if config.db_schemas.contains(&content_profile.to_string()) {
                Ok(content_profile.to_string())
            } else {
                Err(Error::UnacceptableSchema {
                    schemas: config.db_schemas.clone(),
                })
            }
        }
        (_, _, Some(&accept_profile), _) => {
            if config.db_schemas.contains(&accept_profile.to_string()) {
                Ok(accept_profile.to_string())
            } else {
                Err(Error::UnacceptableSchema {
                    schemas: config.db_schemas.clone(),
                })
            }
        }
        _ => Ok(config.db_schemas.get(0).unwrap().clone()),
    }?);
    //println!("{} -> {:#?}", schema_name, db_schema);

    if config.db_schemas.len() > 1 {
        response_headers.push((format!("Content-Profile"), schema_name.clone()));
    }

    // check jwt
    let jwt_claims = match &config.jwt_secret {
        Some(key) => match headers.get("Authorization") {
            Some(&a) => {
                let token_str: Vec<&str> = a.split(' ').collect();
                match token_str[..] {
                    ["Bearer", t] | ["bearer", t] => {
                        let validation = Validation {
                            validate_exp: false,
                            ..Default::default()
                        };
                        match decode::<JsonValue>(
                            t,
                            &DecodingKey::from_secret(key.as_bytes()),
                            &validation,
                        ) {
                            Ok(c) => {
                                if let Some(exp) = c.claims.get("exp") {
                                    if from_value::<u64>(exp.clone()).context(JsonSerialize)?
                                        < get_current_timestamp() - 1
                                    {
                                        return Err(Error::JwtTokenInvalid {
                                            message: format!("JWT expired"),
                                        });
                                    }
                                }
                                Ok(Some(c.claims))
                            }
                            Err(err) => match *err.kind() {
                                ErrorKind::InvalidToken => Err(Error::JwtTokenInvalid {
                                    message: format!("{}", err),
                                }),
                                _ => Err(Error::JwtTokenInvalid {
                                    message: format!("{}", err),
                                }),
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
        Some(claims) => match select(&claims, format!("${}", config.role_claim_key).as_str()) {
            Ok(v) => match &v[..] {
                [JsonValue::String(s)] => Ok((s, true)),
                _ => Ok((&config.db_anon_role, false)),
            },
            Err(e) => Err(Error::JwtTokenInvalid {
                message: format!("{}", e),
            }),
        },
        None => Ok((&config.db_anon_role, false)),
    }?;

    // parse request and generate the query
    let request = parse(
        schema_name,
        root,
        db_schema,
        method,
        path,
        parameters,
        body,
        headers,
        cookies,
        config.db_max_rows,
    )?;
    //println!("request: \n{:#?}", request);

    let readonly = match (method, &request) {
        (&Method::GET, _) => true,
        //TODO!!! optimize not volatile function call can be read only
        //(&Method::POST, ApiRequest { query: FunctionCall {..}, .. }) => true,
        _ => false,
    };

    #[cfg(feature = "postgresql")]
    let response = query_postgresql(
        method,
        pool,
        readonly,
        authenticated,
        schema_name,
        &request,
        role,
        &jwt_claims,
        config,
    )
    .await?;

    #[cfg(feature = "sqlite")]
    let response = task::block_in_place(|| {
        query_sqlite(
            method,
            pool,
            readonly,
            authenticated,
            schema_name,
            &request,
            role,
            &jwt_claims,
            config,
        )
    })?;

    #[cfg(feature = "clickhouse")]
    let response = ApiResponse {
        page_total: 0,
        total_result_set: None,
        top_level_offset: 0,
        response_headers: None,
        response_status: None,
        body: "".to_string(),
    };

    // create and return the response to the client
    let page_total = response.page_total;
    let total_result_set = response.total_result_set;
    let top_level_offset = response.top_level_offset;
    let content_type = match (&request.accept_content_type, &request.query.node) {
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

    let content_range = match (method, &request.query.node, page_total, total_result_set) {
        (&Method::POST, Insert { .. }, _pt, t) => content_range_header(1, 0, t),
        (&Method::DELETE, Delete { .. }, _pt, t) => content_range_header(1, 0, t),
        (_, _, pt, t) => content_range_header(top_level_offset, top_level_offset + pt - 1, t),
    };

    response_headers.push((format!("Content-Range"), content_range));
    if let Some(response_headers_str) = response.response_headers {
        //println!("response_headers_str: {:?}", response_headers_str);
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

    let mut status = match (
        method,
        &request.query.node,
        page_total,
        total_result_set,
        &request.preferences,
    ) {
        (&Method::POST, Insert { .. }, ..) => 201,
        (
            &Method::DELETE,
            Delete { .. },
            ..,
            Some(Preferences {
                representation: Some(Representation::Full),
                ..
            }),
        ) => 200,
        (&Method::DELETE, Delete { .. }, ..) => 204,
        (&Method::PATCH, Update { columns, .. }, 0, _, _) if columns.len() > 0 => 404,
        (
            &Method::PATCH,
            Update { .. },
            ..,
            Some(Preferences {
                representation: Some(Representation::Full),
                ..
            }),
        ) => 200,
        (&Method::PATCH, Update { .. }, ..) => 204,
        (
            &Method::PUT,
            Insert { .. },
            ..,
            Some(Preferences {
                representation: Some(Representation::Full),
                ..
            }),
        ) => 200,
        (&Method::PUT, Insert { .. }, ..) => 204,
        (.., pt, t, _) => content_range_status(top_level_offset, top_level_offset + pt - 1, t),
    };

    if let Some(Preferences {
        resolution: Some(r),
        ..
    }) = request.preferences
    {
        response_headers.push((
            "Preference-Applied".to_string(),
            match r {
                MergeDuplicates => "resolution=merge-duplicates".to_string(),
                IgnoreDuplicates => "resolution=ignore-duplicates".to_string(),
            },
        ));
    }

    let response_status: Option<String> = response.response_status;
    if let Some(response_status_str) = response_status {
        status = response_status_str
            .parse::<u16>()
            .map_err(|_| Error::GucStatusError)?;
    }

    Ok((status, content_type, response_headers, response.body))
}

fn content_range_header(lower: i64, upper: i64, total: Option<i64>) -> String {
    let range_string = if total != Some(0) && lower <= upper {
        format!("{}-{}", lower, upper)
    } else {
        format!("*")
    };
    let total_string = match total {
        Some(t) => format!("{}", t),
        None => format!("*"),
    };
    format!("{}/{}", range_string, total_string)
}

fn content_range_status(lower: i64, upper: i64, total: Option<i64>) -> u16 {
    match (lower, upper, total) {
        //(_, _, None) => 200,
        (l, _, Some(t)) if l > t => 406,
        (l, u, Some(t)) if (1 + u - l) < t => 206,
        _ => 200,
    }
}
