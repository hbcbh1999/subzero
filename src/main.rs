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
                                "name":"api",
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
                                                "table":["api","users"],
                                                "columns": ["billing_address_id"],
                                                "referenced_table":["api","addresses"],
                                                "referenced_columns": ["id"]
                                            },
                                            {
                                                "name":"shipping_address_id_fk",
                                                "table":["api","users"],
                                                "columns": ["shipping_address_id"],
                                                "referenced_table":["api","addresses"],
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
                                                "table":["api","projects"],
                                                "columns": ["client_id"],
                                                "referenced_table":["api","clients"],
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
                                                "table":["api","tasks"],
                                                "columns": ["project_id"],
                                                "referenced_table":["api","projects"],
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
                                                "table":["api","users_tasks"],
                                                "columns": ["task_id"],
                                                "referenced_table":["api","tasks"],
                                                "referenced_columns": ["id"]
                                            },
                                            {
                                                "name":"user_id_fk",
                                                "table":["api","users_tasks"],
                                                "columns": ["user_id"],
                                                "referenced_table":["api","users"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                "#;

// use rocket::http::{CookieJar};
use rocket::Config as RocketConfig;
use std::collections::HashMap;
use rocket::{State,};
use serde::Deserialize;
use figment::{Figment, Profile, };
use figment::providers::{Env, Toml, Format};
//use figment::value::{Map, Dict, magic::RelativePathBuf};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{NoTls, IsolationLevel};
use futures::future;
// use std::future::Future;

// mod api;
// mod error;
// mod schema;
// mod parser;
// mod executor;
// mod dynamic_statement;

//use subzero::api::*;
use subzero::schema::DbSchema;
use subzero::parser::postgrest::parse;
use subzero::dynamic_statement::generate;
use subzero::formatter::postgresql::format;
//use postgres_types::{ToSql, Unknown};

use http::Method;
// use parser::postgrest::PostgrestRequest;
//use rocket_sync_db_pools::{database, postgres};

// #[database("pg")]
// struct PgDatabase(postgres::Client);


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
        //conn: PgDatabase,
        // cookies: &CookieJar<'_>,
        //config: &Config,
        //request: PostgrestRequest,
) -> String {
    let schema_name = config.db_schema.clone();
    let request = parse(&schema_name, &root, db_schema, &Method::GET, parameters.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect(), None).unwrap();
    let (statement, parameters, _) = generate(format(&schema_name, &request.query));
    let mut client = pool.get().await.unwrap();

    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .read_only(true)
        .start()
        .await
        .unwrap();

    let (env_stm, main_stm) = future::try_join(
            transaction.prepare_cached("select set_config('myvar.test', 'off', true)"),
            transaction.prepare_cached(statement.as_str())
        ).await.unwrap();
    let (_, rows) = future::try_join(
        transaction.query(&env_stm, &[]),
        transaction.query(&main_stm, parameters.as_slice())
    ).await.unwrap();

    transaction.commit().await.unwrap();

    let body: String = rows[0].get("body");

    return body;
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
    println!("{:?}", pg_uri);
    //println!("{:?}", app_config.url);
    //let a = config.
    let pg_config = pg_uri.parse::<tokio_postgres::Config>();
    // let mut c = pg_config.unwrap();
    // if let None = c.get_application_name() {
    //     c.application_name("pg-event-proxy");
    // }
    
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast
    };
    let mgr = Manager::from_config(pg_config.unwrap(), NoTls, mgr_config);
    let pool = Pool::builder(mgr).max_size(10).build().unwrap();

    rocket::custom(config)
        .manage(db_schema)
        .manage(app_config)
        .manage(pool)
        //.attach(PgDatabase::fairing())
        .mount("/", routes![index])
        .mount("/rest", routes![get])
}


// #[rocket::main]
// async fn main() {
//     rocket::build()
//         //.attach(PgDatabase::fairing())
//         .mount("/", routes![index])
//         .mount("/rest", routes![get])
//         .launch().await;
// }


