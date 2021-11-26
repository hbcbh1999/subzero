#![feature(drain_filter)]
#![feature(in_band_lifetimes)]

#[macro_use] extern crate rocket;
// #[macro_use] extern crate combine;
// #[macro_use] extern crate serde_derive;
//#[macro_use] extern crate simple_error;
// use snafu::ResultExt;
use snafu::{ResultExt};

use http::Method;
use rocket::http::{CookieJar};
use rocket::{Rocket, Build, Config as RocketConfig};
use subzero::api::ApiRequest;
// use core::slice::SlicePattern;
use std::collections::HashMap;
use rocket::{State,};

use figment::{Figment, Profile, };
use figment::providers::{Env, Toml, Format};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{NoTls, IsolationLevel, types::ToSql};
//use futures::future;
use std::fs;


use subzero::schema::DbSchema;
use subzero::parser::postgrest::parse;
use subzero::dynamic_statement::{generate, SqlSnippet, param, JoinIterator};
use subzero::formatter::postgresql::{main_query};
use subzero::error::{Result};
use subzero::error::*;
use subzero::config::{Config,  SchemaStructure::*};
// use ref_cast::RefCast;
use rocket::http::{Header, ContentType};


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

fn get_postgrest_env(search_path: &Vec<String>, request: &ApiRequest) -> HashMap<String, String>{
    let mut env = HashMap::new();
    env.insert("search_path".to_string(), search_path.join(", ").to_string());
    env.extend(request.headers.iter().map(|(k,v)| (format!("pgrst.header.{}",k),v.to_string())));
    env.extend(request.cookies.iter().map(|(k,v)| (format!("pgrst.cookie.{}",k),v.to_string())));
    env
}

fn get_postgrest_env_query<'a>(env: &'a HashMap<String, String>) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)> {
    "select " + env.iter().map(|(k,v)| "set_config("+param(k as &(dyn ToSql + Sync + 'a))+", "+ param(v as &(dyn ToSql + Sync + 'a))+", true)"  ).join(",")
}

#[derive(Responder, Debug)]
struct ApiResponse {
    body: String,
    content_type: ContentType,
    content_range: Header<'static>
    //status: Header<'static>,
    //headers: Vec<Header<'r>>,
}

async fn handle_postgrest_request(
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
    //let schema_name = config.db_schema.clone();
    let request = parse(schema_name, root, db_schema, method, parameters, body, headers, cookies)?;
    let (main_statement, main_parameters, _) = generate(main_query(&schema_name, &request.query));
    let env = get_postgrest_env(&vec![schema_name.clone()], &request);
    let (env_statement, env_parameters, _) = generate(get_postgrest_env_query(&env));
    let mut client = pool.get().await.context(DbPoolError)?;

    println!("statements:====================\n{}\n{:?}\n===\n{}\n{:?}\n=================", env_statement, env_parameters, main_statement,main_parameters);
     

    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .read_only(true)
        .start()
        .await.context(DbError)?;

    //TODO!!! optimize this so we run both queries in paralel
    let env_stm = transaction.prepare_cached(env_statement.as_str()).await.context(DbError)?;
    let _ = transaction.query(&env_stm, env_parameters.as_slice()).await.context(DbError)?;
    let main_stm = transaction.prepare_cached(main_statement.as_str()).await.context(DbError)?;
    let rows = transaction.query(&main_stm, main_parameters.as_slice()).await.context(DbError)?;

    // let (env_stm, main_stm) = future::try_join(
    //         transaction.prepare_cached(env_statement.as_str()),
    //         transaction.prepare_cached(main_statement.as_str())
    //     ).await.context(DbError)?;
    
    // let (_, rows) = future::try_join(
    //     transaction.query(&env_stm, env_parameters.as_slice()),
    //     transaction.query(&main_stm, main_parameters.as_slice())
    // ).await.context(DbError)?;
    
    transaction.commit().await.context(DbError)?;

    let body: String = rows[0].get("body");
    let page_total: i64 = rows[0].get("page_total");
    
    Ok(ApiResponse {
        body,
        content_type: ContentType::JSON,
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
    
    Ok(handle_postgrest_request(&schema_name, &root, &Method::GET, parameters, db_schema, pool, None, headers, cookies).await?)
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
    Ok(handle_postgrest_request(&schema_name, &root, &Method::POST, parameters, db_schema, pool, Some(&body), headers, cookies).await?)
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
                            Err(e) => Err(e).context(DbError)
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
        .mount("/rest", routes![get,post]))
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

#[cfg_attr(test, macro_use)]
extern crate lazy_static;

#[cfg(test)]
#[path = "../tests/basic/mod.rs"]
mod basic;

#[cfg(test)]
#[path = "../tests/postgrest/mod.rs"]
mod postgrestinegration;
