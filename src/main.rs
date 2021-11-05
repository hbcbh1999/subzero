#![feature(drain_filter)]
#![feature(in_band_lifetimes)]

#[macro_use] extern crate rocket;
// #[macro_use] extern crate combine;
// #[macro_use] extern crate lazy_static;
// #[macro_use] extern crate serde_derive;
//#[macro_use] extern crate simple_error;
pub static JSON_SCHEMA:&str = r#"
                    {
                        "schemas":[
                            {
                                "name":"public",
                                "objects":[
                                    {
                                        "kind":"view",
                                        "name":"addresses",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"location", "data_type":"text" }
                                        ],
                                        "foreign_keys":[]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"users",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"name", "data_type":"text" },
                                            { "name":"billing_address_id", "data_type":"int" },
                                            { "name":"shipping_address_id", "data_type":"int" }
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"billing_address_id_fk",
                                                "table":["public","users"],
                                                "columns": ["billing_address_id"],
                                                "referenced_table":["public","addresses"],
                                                "referenced_columns": ["id"]
                                            },
                                            {
                                                "name":"shipping_address_id_fk",
                                                "table":["public","users"],
                                                "columns": ["shipping_address_id"],
                                                "referenced_table":["public","addresses"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"clients",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"name", "data_type":"text" }
                                        ],
                                        "foreign_keys":[]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"projects",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"client_id", "data_type":"int" },
                                            { "name":"name", "data_type":"text" }
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"client_id_fk",
                                                "table":["public","projects"],
                                                "columns": ["client_id"],
                                                "referenced_table":["public","clients"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"tasks",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"project_id", "data_type":"int" },
                                            { "name":"name", "data_type":"text" }
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"project_id_fk",
                                                "table":["public","tasks"],
                                                "columns": ["project_id"],
                                                "referenced_table":["public","projects"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"users_tasks",
                                        "columns":[
                                            { "name":"task_id", "data_type":"int", "primary_key":true },
                                            { "name":"user_id", "data_type":"int", "primary_key":true }
                                            
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"task_id_fk",
                                                "table":["public","users_tasks"],
                                                "columns": ["task_id"],
                                                "referenced_table":["public","tasks"],
                                                "referenced_columns": ["id"]
                                            },
                                            {
                                                "name":"user_id_fk",
                                                "table":["public","users_tasks"],
                                                "columns": ["user_id"],
                                                "referenced_table":["public","users"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                "#;

use http::Method;
use rocket::http::{CookieJar};
use rocket::Config as RocketConfig;
use subzero::api::ApiRequest;
use std::collections::HashMap;
use rocket::{State,};
use serde::Deserialize;
use figment::{Figment, Profile, };
use figment::providers::{Env, Toml, Format};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{NoTls, IsolationLevel, types::ToSql};
use futures::future;

use subzero::schema::DbSchema;
use subzero::parser::postgrest::parse;
use subzero::dynamic_statement::{generate, SqlSnippet, param, JoinIterator};
use subzero::formatter::postgresql::{main_query, pool_err_to_app_err,pg_error_to_app_err};
use subzero::error::{Result};

pub struct Headers<'r>(&'r rocket::http::HeaderMap<'r>);

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

fn get_postgrest_env(request: &ApiRequest) -> HashMap<String, String>{
    let mut env = HashMap::new();
    env.extend(request.headers.iter().map(|(k,v)| (format!("pgrst.header.{}",k),v.to_string())));
    env.extend(request.cookies.iter().map(|(k,v)| (format!("pgrst.cookie.{}",k),v.to_string())));
    env
}

fn get_postgrest_env_query<'a>(env: &'a HashMap<String, String>) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)> {
    "select " + env.iter().map(|(k,v)| "set_config("+param(k as &(dyn ToSql + Sync + 'a))+", "+ param(v as &(dyn ToSql + Sync + 'a))+", true)"  ).join(",")
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
) -> Result<String> {
    //let schema_name = config.db_schema.clone();
    let request = parse(schema_name, root, db_schema, method, parameters, body, headers, cookies)?;
    let (main_statement, main_parameters, _) = generate(main_query(&schema_name, &request.query));
    let env = get_postgrest_env(&request);
    let (env_statement, env_parameters, _) = generate(get_postgrest_env_query(&env));
    let mut client = pool.get().await.map_err(pool_err_to_app_err)?;

    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .read_only(true)
        .start()
        .await.map_err(pg_error_to_app_err)?;

    let (env_stm, main_stm) = future::try_join(
            transaction.prepare_cached(env_statement.as_str()),
            transaction.prepare_cached(main_statement.as_str())
        ).await.map_err(pg_error_to_app_err)?;
    let (_, rows) = future::try_join(
        transaction.query(&env_stm, env_parameters.as_slice()),
        transaction.query(&main_stm, main_parameters.as_slice())
    ).await.map_err(pg_error_to_app_err)?;

    transaction.commit().await.map_err(pg_error_to_app_err)?;

    let body: String = rows[0].get("body");

    return Ok(body);
}

#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[get("/<root>?<parameters..>")]
async fn get<'a>(
        root: String,
        parameters: HashMap<String, String>,
        db_schema: &State<DbSchema>,
        config: &State<Config>,
        pool: &State<Pool>,
        cookies: &CookieJar<'_>,
        headers: Headers<'_>,
) -> Result<String> {
    let schema_name = config.db_schema.clone();
    let parameters = parameters.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
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
        parameters: HashMap<String, String>,
        db_schema: &State<DbSchema>,
        config: &State<Config>,
        pool: &State<Pool>,
        body: String,
        cookies: &CookieJar<'_>,
        headers: Headers<'_>,
) -> Result<String> {
    let schema_name = config.db_schema.clone();
    let parameters = parameters.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
    let cookies = cookies.iter().map(|c| (c.name(), c.value())).collect::<HashMap<_,_>>();
    let headers = headers.iter()
        .map(|h| (h.name().as_str().to_string(), h.value().to_string()))
        .collect::<HashMap<_,_>>();
    let headers = headers.iter().map(|(k,v)| (k.as_str(),v.as_str()))
        .collect::<HashMap<_,_>>();
    Ok(handle_postgrest_request(&schema_name, &root, &Method::POST, parameters, db_schema, pool, Some(&body), headers, cookies).await?)
}

#[derive(Deserialize, Debug)]
struct Config {
    db_uri: String,
    db_schema: String,
}


#[launch]
fn rocket() -> _ {
    env_logger::init();

    let db_schema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).expect("failed to parse json schema");
    
    let config = Figment::from(RocketConfig::default())
        .merge(Toml::file(Env::var_or("SUBZERO_CONFIG", "config.toml")).nested())
        .merge(Env::prefixed("SUBZERO_").ignore(&["PROFILE"]).global())
        .select(Profile::from_env_or("SUBZERO_PROFILE", Profile::const_new("debug")));
    
    let app_config:Config = config.extract().expect("config");
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

    rocket::custom(config)
        .manage(db_schema)
        .manage(app_config)
        .manage(pool)
        .mount("/", routes![index])
        .mount("/rest", routes![get,post])
}


#[cfg(test)]
mod test {
    use super::rocket;
    use rocket::local::blocking::Client;
    use rocket::http::Status;

    #[test]
    fn hello_world() {
        let client = Client::tracked(rocket()).expect("valid rocket instance");
        let response = client.get("/").dispatch();
        assert_eq!(response.status(), Status::Ok);
        assert_eq!(response.into_string().unwrap(), "Hello, world!");
    }

    #[test]
    fn simple_get(){
        let client = Client::tracked(rocket()).expect("valid rocket instance");
        let response = client.get("/rest/projects?select=id,name&id=gt.1&name=eq.IOS").dispatch();
        assert_eq!(response.status(), Status::Ok);
        assert_eq!(response.into_string().unwrap(), "Hello, world!");
    }
}