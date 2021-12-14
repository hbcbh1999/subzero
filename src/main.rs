#![feature(drain_filter)]
//#![feature(in_band_lifetimes)]

#[macro_use] extern crate rocket;
#[macro_use] extern crate lazy_static;

// #[macro_use] extern crate combine;
// #[macro_use] extern crate serde_derive;
//#[macro_use] extern crate simple_error;
// use snafu::ResultExt;
use snafu::{ResultExt};

use http::Method;
use rocket::http::{CookieJar};
use rocket::{Rocket, Build, Config as RocketConfig};
use subzero::api::{ApiRequest, Query::*, ResponseContentType::*,};
// use core::slice::SlicePattern;
use std::collections::HashMap;
use rocket::{State,};

use figment::{Figment, Profile, };
use figment::providers::{Env, Toml, Format};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{NoTls, IsolationLevel, types::ToSql};
//use futures::future;
use std::fs;
use serde_json::{from_value};

use std::time::{SystemTime, UNIX_EPOCH};
use subzero::schema::DbSchema;
use subzero::parser::postgrest::parse;
use subzero::dynamic_statement::{generate, SqlSnippet, param, JoinIterator};
use subzero::formatter::postgresql::{main_query};
use subzero::error::{Result};
use subzero::error::*;
use subzero::config::{Config,  SchemaStructure::*};
// use ref_cast::RefCast;
use rocket::http::{Header, ContentType, Status};
//use rocket::response::status;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, DecodingKey, Validation};

use serde_json::{Value as JsonValue};
use jsonpath_lib::select;


pub struct Headers<'r>(&'r rocket::http::HeaderMap<'r>);
use rocket::form::{FromForm, ValueField, DataField, Options, Result as FormResult};

#[derive(Debug)]
#[repr(transparent)]
pub struct QueryStringParameters<'r> (Vec<(&'r str, &'r str)>);

#[rocket::async_trait]
impl<'v> FromForm<'v> for QueryStringParameters<'v> {
    type Context = Vec<(&'v str, &'v str)>;

    fn init(_opts: Options) -> Self::Context {
        vec![]
    }

    fn push_value(ctxt: &mut Self::Context, field: ValueField<'v>) {
        ctxt.push((field.name.source(), field.value));
    }

    async fn push_data(_ctxt: &mut Self::Context, _field: DataField<'v, '_>) {
    }

    fn finalize(this: Self::Context) -> FormResult<'v, Self> {
        Ok(QueryStringParameters(this))
    }
}

#[rocket::async_trait]
impl<'r> rocket::request::FromRequest<'r> for Headers<'r> {
	type Error = std::convert::Infallible;

	async fn from_request(
		req: &'r rocket::Request<'_>,
	) -> rocket::request::Outcome<Self, Self::Error> {
		rocket::request::Outcome::Success(Headers(req.headers()))
	}
}

impl<'r> std::ops::Deref for Headers<'r> {
	type Target = rocket::http::HeaderMap<'r>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}


lazy_static!{
    //static ref STAR: String = "*".to_string();
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

#[derive(Responder, Debug)]
struct ApiResponse {
    response: (Status, (ContentType, String)),
    content_range: Header<'static>,
}


fn get_current_timestamp() -> u64 {
    //TODO!!! optimize this to run once per second
    let start = SystemTime::now();
    start.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs()
}

async fn handle_postgrest_request(
    config: &Config,
    schema_name: &String,
    root: &String,
    method: &Method,
    parameters: Vec<(&str, &str)>,
    db_schema: &State<DbSchema>,
    //config: &State<Config>,
    pool: &State<Pool>,
    body: Option<&String>,
    headers: HashMap<&str, &str>,
    cookies: HashMap<&str, &str>,
) -> Result<ApiResponse> {
    // handle jwt
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
    
    let request = parse(schema_name, root, db_schema, method, parameters, body, headers, cookies)?;
    let (main_statement, main_parameters, _) = generate(main_query(&schema_name, &request));
    let env = get_postgrest_env(role, &vec![schema_name.clone()], &request, &jwt_claims);
    let (env_statement, env_parameters, _) = generate(get_postgrest_env_query(&env));
    let mut client = pool.get().await.context(DbPoolError)?;

    println!("statements:====================\n{}\n{:?}\n===\n{}\n{:?}\n=================", env_statement, env_parameters, main_statement,main_parameters);
     

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
        //println!("pre_request_statement:====================\n{}\n=================", pre_request_statement);
        let pre_request_stm = transaction.prepare_cached(pre_request_statement.as_str()).await.context(DbError {authenticated})?;
        transaction.query(&pre_request_stm, &[]).await.context(DbError {authenticated})?;
        //println!("pre_request_statement qq:====================\n{:?}\n=================", qq);
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

    let body: String = rows[0].get("body");
    let page_total: i64 = rows[0].get("page_total");
    let content_type:ContentType = match ( &request.accept_content_type, &request.query) {
        (SingularJSON, _) |
        (_, FunctionCall { returns_single: true, is_scalar: false, .. })
            => SINGLE_CONTENT_TYPE.clone(),
        
        _ => ContentType::JSON,
    };
    
    println!("jjjjjjj==========={:?}", ( &content_type, &request.accept_content_type, &request.query));
    Ok(ApiResponse {
        response: (Status::Ok, (content_type, body)),
        content_range: Header::new("Content-Range", format!("0-{}/*", page_total - 1))
    })
}

// #[catch(400)]
// fn not_found(req: &Request) -> String {
//     format!("Sorry, '{}' is not a valid path.", req.uri())
// }

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}


#[get("/test?<parameters..>")]
async fn test<'a>( parameters: QueryStringParameters<'a>) -> &'a str {
    println!("parameters top {:#?}", parameters.0);
    "ok"
}

#[get("/<root>?<parameters..>")]
async fn get<'a>(
        root: String,
        parameters: QueryStringParameters<'a>,
        db_schema: &State<DbSchema>,
        config: &State<Config>,
        pool: &State<Pool>,
        cookies: &CookieJar<'_>,
        headers: Headers<'_>,
) -> Result<ApiResponse> {
    let schema_name = config.db_schemas.get(0).unwrap();
    let parameters = parameters.0;
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    
    Ok(handle_postgrest_request(&config, &schema_name, &root, &Method::GET, parameters, db_schema, pool, None, headers, cookies).await?)
}

#[post("/<root>?<parameters..>", data = "<body>")]
async fn post<'a>(
        root: String,
        parameters: QueryStringParameters<'a>,
        db_schema: &State<DbSchema>,
        config: &State<Config>,
        pool: &State<Pool>,
        body: String,
        cookies: &CookieJar<'_>,
        headers: Headers<'_>,
) -> Result<ApiResponse> {
    let schema_name = config.db_schemas.get(0).unwrap();
    let parameters = parameters.0;
    
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    Ok(handle_postgrest_request(&config, &schema_name, &root, &Method::POST, parameters, db_schema, pool, Some(&body), headers, cookies).await?)
}


#[get("/rpc/<root>?<parameters..>")]
async fn rpc_get<'a>(
        root: String,
        parameters: QueryStringParameters<'a>,
        db_schema: &State<DbSchema>,
        config: &State<Config>,
        pool: &State<Pool>,
        cookies: &CookieJar<'_>,
        headers: Headers<'_>,
) -> Result<ApiResponse> {
    let schema_name = config.db_schemas.get(0).unwrap();
    let parameters = parameters.0;
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    
    Ok(handle_postgrest_request(&config, &schema_name, &root, &Method::GET, parameters, db_schema, pool, None, headers, cookies).await?)
}

#[post("/rpc/<root>?<parameters..>", data = "<body>")]
async fn rpc_post<'a>(
        root: String,
        parameters: QueryStringParameters<'a>,
        db_schema: &State<DbSchema>,
        config: &State<Config>,
        pool: &State<Pool>,
        body: String,
        cookies: &CookieJar<'_>,
        headers: Headers<'_>,
) -> Result<ApiResponse> {
    let schema_name = config.db_schemas.get(0).unwrap();
    let parameters = parameters.0;
    
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    Ok(handle_postgrest_request(&config, &schema_name, &root, &Method::POST, parameters, db_schema, pool, Some(&body), headers, cookies).await?)
}


pub async fn start(config: &Figment) -> Result<Rocket<Build>> {
    let app_config:Config = config.extract().expect("config");

    //println!("{:#?}", app_config);
    //setup db connection
    let pg_uri = app_config.db_uri.clone();
    let mut pg_config = pg_uri.parse::<tokio_postgres::Config>().unwrap();
    if let None = pg_config.get_application_name() {
        pg_config.application_name("subzero");
    }
    
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast
    };
    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
    let pool = Pool::builder(mgr).max_size(10).build().unwrap();


    //read db schema
    let db_schema = match &app_config.db_schema_structure {
        SqlFile(f) => match fs::read_to_string(f) {
            Ok(s) => {
                match pool.get().await{
                    Ok(client) => {
                       
    
                        let params = vec![app_config.db_schemas.iter().map(|s| s.as_str()).collect::<Vec<_>>()];
                        let params: Vec<&(dyn ToSql + Sync)> = params.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
                        // let params:Vec<&(dyn ToSql + Sync)> = vec![&params];
                        match client.query(&s, &params).await {
                            Ok(rows) => {
                               
                                let value:&str = rows[0].get(0);
                                //println!("got rows {:?}", value);
                                serde_json::from_str::<DbSchema>(value).context(JsonDeserialize)
                            },
                            Err(e) => Err(e).context(DbError {authenticated:false})
                        }
                        //serde_json::from_str::<DbSchema>("ssss").context(JsonDeserialize)
                    },
                    Err(e) => Err(e).context(DbPoolError)
                }
            },
            Err(e) => Err(e).context(ReadFile {path: f})
        },
        JsonFile(f) => {
            match fs::read_to_string(f) {
                Ok(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize),
                Err(e) => Err(e).context(ReadFile {path: f})
            }
        },
        JsonString(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize)
    }?;
    //let db_schema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).expect("failed to parse json schema");


    Ok(rocket::custom(config)
        .manage(db_schema)
        .manage(app_config)
        .manage(pool)
        .mount("/", routes![index, test])
        .mount("/rest", routes![get,post,rpc_get,rpc_post]))
}

#[launch]
async fn rocket() -> Rocket<Build> {
    //env_logger::init();

    let config = Figment::from(RocketConfig::default())
        .merge(Toml::file(Env::var_or("SUBZERO_CONFIG", "config.toml")).nested())
        .merge(Env::prefixed("SUBZERO_").ignore(&["PROFILE"]).global())
        .select(Profile::from_env_or("SUBZERO_PROFILE", Profile::const_new("debug")));
    
    match start(&config).await {
        Ok(r) => r,
        Err(e) => panic!("{}", e)
    }
}

// #[cfg_attr(test, macro_use)]
// extern crate lazy_static;

#[cfg(test)]
#[path = "../tests/basic/mod.rs"]
mod basic;

#[cfg(test)]
#[path = "../tests/postgrest/core.rs"]
mod postgrest_core;
