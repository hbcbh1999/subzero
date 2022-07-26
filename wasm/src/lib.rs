mod utils;

//use subzero_core::schema::DbSchema;
use subzero_core::{
    // api::{ ContentType, ContentType::*, Preferences, QueryNode::*, Representation, Resolution::*, SelectItem::*},
    // error::{*},
    parser::postgrest::parse,
    schema::DbSchema,
    formatter::{postgresql::{fmt_main_query, generate}, },
};
use wasm_bindgen::{prelude::*, };
use js_sys::{JsString};
use serde_json;
use std::{collections::HashMap, };
use http::Method;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    alert("Hello, wasm!");
}
#[wasm_bindgen]
pub struct Backend {
    db_schema: DbSchema
}

#[wasm_bindgen]
impl Backend {
    pub fn init(s: &str) -> Backend {
        let db_schema = serde_json::from_str(s).expect("invalid schema json");
        Backend { db_schema }
    }
    pub fn get_query(
        &self,
        schema_name: &JsString,
        root: &JsString, 
        method: &JsString, 
        path: &JsString, 
        get: &JsValue, 
        body: &JsValue,
        headers: &JsValue,
        cookies: &JsValue,
    )
    -> String {
        let schema_name: String = schema_name.into();
        let root: String = root.into();
        let method: String = method.into();
        let path: String = path.into();
        let get:Vec<(String, String)> = get.into_serde().unwrap();
        let body:Option<String> = body.into_serde().unwrap();
        let headers: HashMap<String, String> = headers.into_serde().unwrap();
        let cookies: HashMap<String, String> = cookies.into_serde().unwrap();
        let db_schema = &self.db_schema;
        let method = match method.as_str() {
            "get" => Method::GET,
            "post" => Method::POST,
            "put" => Method::PUT,
            "delete" => Method::DELETE,
            "patch" => Method::PATCH,
            _ => Method::GET,
        };
        let max_rows = None;
        let request = parse(&schema_name, &root, db_schema, &method, path, get, body, headers, cookies, max_rows).unwrap();
        let query = fmt_main_query(&request.schema_name, &request).unwrap();
        let (main_statement, _main_parameters, _) = generate(query);
        main_statement
    }
}
