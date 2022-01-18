

use serde_json::{from_value, Value as JsonValue};
use deadpool_postgres::{Pool};
use tokio_postgres::{IsolationLevel, types::ToSql};
use jsonwebtoken::{decode, DecodingKey, Validation, errors::ErrorKind};
use jsonpath_lib::select;
use snafu::{ResultExt};
use http::Method;

use crate::{
    api::{ApiRequest, Query::*, ContentType, ContentType::*,},
    schema::DbSchema,
    error::{*, Result},
    config::{VhostConfig, },
    dynamic_statement::{SqlSnippet, JoinIterator, generate, param, },
    parser::postgrest::parse,
    formatter::postgresql::main_query,
};


use std::{
    collections::{HashMap},
    time::{SystemTime, UNIX_EPOCH}
};


fn get_postgrest_env(role: &String, search_path: &Vec<String>, request: &ApiRequest, jwt_claims: &Option<JsonValue>) -> HashMap<String, String>{
    let mut env = HashMap::new();
    env.insert("role".to_string(), role.clone());
    env.insert("request.method".to_string(), format!("{}", request.method));
    env.insert("request.path".to_string(), format!("{}", request.path));
    //pathSql = setConfigLocal mempty ("request.path", iPath req)
    env.insert("request.jwt.claim.role".to_string(), role.clone());
    env.insert("search_path".to_string(), search_path.join(", ").to_string());
    env.extend(request.headers.iter().map(|(k,v)| (format!("request.header.{}",k.to_lowercase()),v.to_string())));
    env.extend(request.cookies.iter().map(|(k,v)| (format!("request.cookie.{}",k),v.to_string())));
    match jwt_claims {
        Some(v) => {
            match v.as_object() {
                Some(claims) => {
                    env.extend(claims.iter().map(|(k,v)| (
                        format!("request.jwt.claim.{}",k), 
                        match v {JsonValue::String(s) => s.clone(), _=> format!("{}",v)}
                    )));
                }
                None => {}
            }
        }
        None => {}
    }
    env
}

fn get_postgrest_env_query<'a>(env: &'a HashMap<String, String>) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)> {
    "select " + env.iter().map(|(k,v)| "set_config("+param(k as &(dyn ToSql + Sync + 'a))+", "+ param(v as &(dyn ToSql + Sync + 'a))+", true)"  ).join(",")
}

fn get_current_timestamp() -> u64 {
    //TODO!!! optimize this to run once per second
    let start = SystemTime::now();
    start.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs()
}

pub async fn handle_postgrest_request(
    config: &VhostConfig,
    root: &String,
    method: &Method,
    path: String,
    parameters: &Vec<(&str, &str)>,
    db_schema: &DbSchema,
    pool: &Pool,
    body: Option<String>,
    headers: &HashMap<&str, &str>,
    cookies: &HashMap<&str, &str>,
//) -> Result<ApiResponse> {
) -> Result<(u16, ContentType, Vec<(String, String)>, String)> {
    let schema_name = config.db_schemas.get(0).unwrap();

    // check jwt
    let jwt_claims = match &config.jwt_secret {
        Some(key) => {
            match headers.get("Authorization"){
                Some(&a) => {
                    let token_str:Vec<&str> = a.split(' ').collect();
                    match token_str[..] {
                        ["Bearer", t] | ["bearer", t] => {
                            let validation = Validation {validate_exp: false, ..Default::default()};
                            match decode::<JsonValue>(t, &DecodingKey::from_secret(key.as_bytes()), &validation){
                                Ok(c) => {
                                    if let Some(exp) = c.claims.get("exp") {
                                        if from_value::<u64>(exp.clone()).context(JsonSerialize)? < get_current_timestamp() - 1 {
                                            return Err(Error::JwtTokenInvalid {message: format!("JWT expired")});
                                        }
                                    }
                                    Ok(Some(c.claims))
                                },
                                Err(err) => match *err.kind() {
                                    ErrorKind::InvalidToken => Err(Error::JwtTokenInvalid {message: format!("{}", err)}),
                                    //ErrorKind::InvalidIssuer => panic!("Issuer is invalid"), // Example on how to handle a specific error
                                    _ => Err(Error::JwtTokenInvalid {message: format!("{}", err)}),
                                }
                            }
                        },
                        _ => Ok(None)
                    }
                }
                None => Ok(None)
            }
        }
        None => Ok(None)
    }?;

    let (role, authenticated) = match &jwt_claims {
        Some(claims) => {
            match select(&claims, format!("${}", config.role_claim_key).as_str()) {
                Ok(v) => match &v[..] {
                    [JsonValue::String(s)] => Ok((s,true)),
                    _ => Ok((&config.db_anon_role, false))
                }
                Err(e) => Err(Error::JwtTokenInvalid { message: format!("{}", e)})
            }
        }
        None => Ok((&config.db_anon_role, false))
    }?;
    
    // parse request and generate the query
    let request = parse(schema_name, root, db_schema, method, path, parameters, body, headers, cookies)?;
    //println!("request: \n{:#?}", request);
    let (main_statement, main_parameters, _) = generate(main_query(&schema_name, &request));
    println!("main_statement: \n{}\n{:?}", main_statement, main_parameters);
    let env = get_postgrest_env(role, &vec![schema_name.clone()], &request, &jwt_claims);
    let (env_statement, env_parameters, _) = generate(get_postgrest_env_query(&env));
    // println!("env_parameters: \n{:#?}", env_parameters);
    // println!("headers: \n{:#?}", headers);
    // println!("cookies: \n{:#?}", cookies);
    
    
    // fetch response from the database
    let mut client = pool.get().await.context(DbPoolError)?;
    let readonly = match (method, &request){
        (&Method::GET, _) => true,
        //TODO!!! optimize not volatile function call can be read only
        //(&Method::POST, ApiRequest { query: FunctionCall {..}, .. }) => true,
        _ => false,
    };
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .read_only(readonly)
        .start()
        .await.context(DbError {authenticated})?;

    //TODO!!! optimize this so we run both queries in paralel
    let env_stm = transaction.prepare_cached(env_statement.as_str()).await.context(DbError {authenticated})?;
    let _ = transaction.query(&env_stm, env_parameters.as_slice()).await.context(DbError {authenticated})?;

    if let Some((s,f)) = &config.db_pre_request {
        let fn_schema = match s.as_str() {
            "" => schema_name,
            _ => &s
        };

        let pre_request_statement = format!(r#"select "{}"."{}"()"#, fn_schema, f);
        let pre_request_stm = transaction.prepare_cached(pre_request_statement.as_str()).await.context(DbError {authenticated})?;
        transaction.query(&pre_request_stm, &[]).await.context(DbError {authenticated})?;
    }

    let main_stm = transaction.prepare_cached(main_statement.as_str()).await.context(DbError {authenticated})?;
    let rows = transaction.query(&main_stm, main_parameters.as_slice()).await.context(DbError {authenticated})?;

    // let (env_stm, main_stm) = future::try_join(
    //         transaction.prepare_cached(env_statement.as_str()),
    //         transaction.prepare_cached(main_statement.as_str())
    //     ).await.context(DbError)?;
    
    // let (_, rows) = future::try_join(
    //     transaction.query(&env_stm, env_parameters.as_slice()),
    //     transaction.query(&main_stm, main_parameters.as_slice())
    // ).await.context(DbError)?;
    if config.db_tx_rollback {
        transaction.rollback().await.context(DbError {authenticated})?;
    }
    else {
        transaction.commit().await.context(DbError {authenticated})?;
    }


    // create and return the response to the client
    let page_total: i64 = rows[0].get("page_total");
    let content_type = match ( &request.accept_content_type, &request.query) {
        (SingularJSON, _) |
        (_, FunctionCall { returns_single: true, is_scalar: false, .. })
            => SingularJSON,
        (TextCSV, _) => TextCSV,
        _ => ApplicationJSON,
    };
    //let mut headers = vec![Header::new("Content-Range", format!("0-{}/*", page_total - 1))];
    let mut headers = vec![(format!("Content-Range"), format!("0-{}/*", page_total - 1))];
    if let Some(response_headers_str) = rows[0].get("response_headers") {
        //println!("response_headers_str: {:?}", response_headers_str);
        match serde_json::from_str(response_headers_str) {
            Ok(JsonValue::Array(headers_json)) =>  {
                for h in headers_json {
                    match h {
                        JsonValue::Object(o) => {
                            for (k,v) in o.into_iter() {
                                match v {
                                    JsonValue::String(s) => {
                                        headers.push((k, s));
                                        Ok(())
                                    }
                                    _ => Err(Error::GucHeadersError)
                                }?
                            }
                            Ok(())
                        }
                        _ => Err(Error::GucHeadersError)
                    }?
                }
                Ok(())
            },
            _ => Err(Error::GucHeadersError),
        }?
    }
    //let mut status = Status::Ok;
    let mut status = match (method, &request.query) {
        (&Method::POST, Insert {..}) => 201,
        _ => 200,
    };

    let response_status:Option<&str> = rows[0].get("response_status");
    if let Some(response_status_str) = response_status {
        //status = Status::from_code(response_status_str.parse::<u16>().map_err(|_| Error::GucStatusError)?).context(GucStatusError)?;
        status = response_status_str.parse::<u16>().map_err(|_| Error::GucStatusError)?;
    }

    let body: String = rows[0].get("body");
    // Ok(ApiResponse {
    //     response: (status, (content_type, body)),
    //     headers
    // })

    Ok((status, content_type, headers, body))
}
