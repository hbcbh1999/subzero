#![feature(drain_filter)]
#![feature(in_band_lifetimes)]

#[macro_use] extern crate rocket;
#[macro_use] extern crate combine;
#[macro_use] extern crate lazy_static;
// #[macro_use] extern crate serde_derive;
//#[macro_use] extern crate simple_error;

// use rocket::http::{CookieJar};
// use rocket::Config;
//use std::collections::HashMap;

mod api;
mod error;
mod schema;
mod parser;
mod executor;

use http::Method;
// use parser::postgrest::PostgrestRequest;
use rocket_sync_db_pools::{database, postgres};

#[database("pg")]
struct PgDatabase(postgres::Client);


#[get("/")]
fn index() -> &'static str {
    let _json_schema = r#"
        {"schemas":[]}
    "#;
    let schema = "api".to_string();
    let root = "projects".to_string();
    let db_schema  = serde_json::from_str::<schema::DbSchema>(_json_schema).unwrap();
    let request= parser::postgrest::parse(&schema, root, &db_schema, &Method::GET, vec![
        ("select", "id,name,child(id)"),
        ("id","not.gt.10"),
        ("child.id","lt.5"),
        ("not.or", "(id.eq.11,id.eq.12)"),
        ]).unwrap();
    let _q = executor::postgresql::fmt_query(&schema, &request.query);
    "Hello, world!"
    
}

// #[get("/<root>?<parameters..>")]
// pub fn get(
//         root: String,
//         parameters: HashMap<String, String>, 
//         // cookies: &CookieJar<'_>,
//         // config: &Config,
//         //request: PostgrestRequest,
// ) -> String {
//     // let PostgrestRequest( api_request ) = request;
//     // println!("root {:?}", root);
//     // println!("parameters {:?}", parameters);
//     // println!("cookies {:?}", cookies);
//     // println!("config {:?}", config);
//     // println!("{:?}", api_request);
//     let x= parser::postgrest::parse(vec![
//         ("select", "id,name,child(id)"),
//         ("id","not.gt.10"),
//         ("child.id","lt.5"),
//         ("not.or", "(id.eq.11,id.eq.12)"),
//         ]);
//     return "Ok".to_string();
// }



// #[rocket::main]
// async fn main() {
//     rocket::build()
//         //.attach(PgDatabase::fairing())
//         .mount("/", routes![index])
//         .mount("/rest", routes![get])
//         .launch().await;
// }


#[launch]
fn rocket() -> _ {
    rocket::build()
        //.attach(PgDatabase::fairing())
        .mount("/", routes![index])
        //.mount("/rest", routes![get])
}

