#![feature(drain_filter)]
#[macro_use] extern crate rocket;
#[macro_use] extern crate lazy_static;

use serde_json::{from_value, Value as JsonValue};
use deadpool_postgres::{Pool};
use tokio_postgres::{IsolationLevel, types::ToSql};
use jsonwebtoken::{decode, DecodingKey, Validation, errors::ErrorKind};
use jsonpath_lib::select;
use snafu::{ResultExt,};
use http::Method;
//use tokio;
use dashmap::{DashMap, };

use rocket::{
    http::{Header, ContentType, Status, CookieJar,},
    Rocket, Build, Config as RocketConfig, State,
};

use subzero::{
    api::{ApiRequest, Query::*, ResponseContentType::*,},
    schema::DbSchema,
    error::{*, Result},
    config::{Config, VhostConfig, },
    dynamic_statement::{SqlSnippet, JoinIterator, generate, param, },
    rocket_util::{AllHeaders, QueryString, ApiResponse, Vhost},
    vhosts::{VhostResources, get_resources, create_resources,},
    parser::postgrest::parse,
    formatter::postgresql::main_query,
};

use figment::{
    providers::{Env, Toml, Format},
    Figment, Profile, 
};

use std::{
    sync::{Arc,},
    collections::{HashMap},
    time::{SystemTime, UNIX_EPOCH}
};

lazy_static!{
    static ref SINGLE_CONTENT_TYPE: ContentType = ContentType::parse_flexible("application/vnd.pgrst.object+json").unwrap();
}

fn get_postgrest_env(role: &String, search_path: &Vec<String>, request: &ApiRequest, jwt_claims: &Option<JsonValue>) -> HashMap<String, String>{
    let mut env = HashMap::new();
    env.insert("role".to_string(), role.clone());
    env.insert("search_path".to_string(), search_path.join(", ").to_string());
    env.extend(request.headers.iter().map(|(k,v)| (format!("request.header.{}",k),v.to_string())));
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

async fn handle_postgrest_request(
    config: &VhostConfig,
    root: &String,
    method: &Method,
    parameters: &Vec<(&str, &str)>,
    db_schema: &DbSchema,
    pool: &Pool,
    body: Option<&String>,
    headers: &HashMap<&str, &str>,
    cookies: &HashMap<&str, &str>,
) -> Result<ApiResponse> {
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
    let request = parse(schema_name, root, db_schema, method, parameters, body, headers, cookies)?;
    //println!("request: \n{:#?}", request);
    let (main_statement, main_parameters, _) = generate(main_query(&schema_name, &request));
    //println!("main_statement: \n{}", main_statement);
    let env = get_postgrest_env(role, &vec![schema_name.clone()], &request, &jwt_claims);
    let (env_statement, env_parameters, _) = generate(get_postgrest_env_query(&env));
    
    // fetch response from the database
    let mut client = pool.get().await.context(DbPoolError)?;
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .read_only(true)
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
    
    transaction.commit().await.context(DbError {authenticated})?;


    // create and return the response to the client
    let body: String = rows[0].get("body");
    let page_total: i64 = rows[0].get("page_total");
    let content_type:ContentType = match ( &request.accept_content_type, &request.query) {
        (SingularJSON, _) |
        (_, FunctionCall { returns_single: true, is_scalar: false, .. })
            => SINGLE_CONTENT_TYPE.clone(),
        (TextCSV, _) => ContentType::CSV,
        _ => ContentType::JSON,
    };
    
    Ok(ApiResponse {
        response: (Status::Ok, (content_type, body)),
        content_range: Header::new("Content-Range", format!("0-{}/*", page_total - 1))
    })
}

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/<root>?<parameters..>")]
async fn get<'a>(
        root: String,
        parameters: QueryString<'a>,
        cookies: &CookieJar<'a>,
        headers: AllHeaders<'a>,
        vhost: Vhost<'a>,
        vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {

    let resources = get_resources(&vhost, vhosts)?;
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    Ok(handle_postgrest_request(&resources.config, &root, &Method::GET, &parameters, &resources.db_schema, &resources.db_pool, None, &headers, &cookies).await?)
}

#[post("/<root>?<parameters..>", data = "<body>")]
async fn post<'a>(
        root: String,
        parameters: QueryString<'a>,
        body: String,
        cookies: &CookieJar<'a>,
        headers: AllHeaders<'a>,
        vhost: Vhost<'a>,
        vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {
    let resources = get_resources(&vhost, vhosts)?;
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    Ok(handle_postgrest_request(&resources.config, &root, &Method::POST, &parameters, &resources.db_schema, &resources.db_pool, Some(&body), &headers, &cookies).await?)
}

#[get("/rpc/<root>?<parameters..>")]
async fn rpc_get<'a>(
        root: String,
        parameters: QueryString<'a>,
        cookies: &CookieJar<'a>,
        headers: AllHeaders<'a>,
        vhost: Vhost<'a>,
        vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {
    let resources = get_resources(&vhost, vhosts)?;
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    Ok(handle_postgrest_request(&resources.config, &root, &Method::GET, &parameters, &resources.db_schema, &resources.db_pool, None, &headers, &cookies).await?)
}

#[post("/rpc/<root>?<parameters..>", data = "<body>")]
async fn rpc_post<'a>(
        root: String,
        parameters: QueryString<'a>,
        body: String,
        cookies: &CookieJar<'a>,
        headers: AllHeaders<'a>,
        vhost: Vhost<'a>,
        vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {
    let resources = get_resources(&vhost, vhosts)?;
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    Ok(handle_postgrest_request(&resources.config, &root, &Method::POST, &parameters, &resources.db_schema, &resources.db_pool, Some(&body), &headers, &cookies).await?)
}

async fn start() -> Result<Rocket<Build>> {

    #[cfg(debug_assertions)]
    let profile = RocketConfig::DEBUG_PROFILE;

    #[cfg(not(debug_assertions))]
    let profile = RocketConfig::RELEASE_PROFILE;

    let config = Figment::from(RocketConfig::default())
        .merge(Toml::file(Env::var_or("SUBZERO_CONFIG", "config.toml")).nested())
        .merge(Env::prefixed("SUBZERO_").split("__").ignore(&["PROFILE"]).global())
        .select(Profile::from_env_or("SUBZERO_PROFILE", profile));
    
    let app_config:Config = config.extract().expect("config");
    let vhost_resources = Arc::new(DashMap::new());
    
    for (vhost, vhost_config) in app_config.vhosts {
        let vhost_resources = vhost_resources.clone();
        //tokio::spawn(async move {
            //sleep(Duration::from_millis(30 * 1000)).await;
            match create_resources(&vhost, vhost_config, vhost_resources).await {
                Ok(_) => println!("[{}] loaded config", vhost),
                Err(e) => println!("[{}] config load failed ({})", vhost, e)
            }
        //});
    }

    Ok(rocket::custom(config)
        .manage(vhost_resources)
        .mount("/", routes![index])
        .mount("/rest", routes![get,post,rpc_get,rpc_post]))
}

#[launch]
async fn rocket() -> Rocket<Build> {

    match start().await {
        Ok(r) => r,
        Err(e) => panic!("{}", e)
    }
}


#[cfg(test)]
#[path = "../tests/basic/mod.rs"]
mod basic;

#[cfg(test)]
#[path = "../tests/postgrest/core.rs"]
mod postgrest_core;
