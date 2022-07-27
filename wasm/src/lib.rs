mod utils;

//use subzero_core::schema::DbSchema;
use subzero_core::{
    // api::{ ContentType, ContentType::*, Preferences, QueryNode::*, Representation, Resolution::*, SelectItem::*},
    error::{Error, },
    parser::postgrest::parse,
    schema::DbSchema,
    formatter::{postgresql::{fmt_main_query, generate}, },
};
// use snafu::Snafu;
// use std::fmt;
use wasm_bindgen::{prelude::*, };
use js_sys::{JsString, Error as JsError, Array, Map};
use serde_json;
use std::{collections::HashMap, };
use http::Method;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
extern "C" {
    // Use `js_namespace` here to bind `console.log(..)` instead of just
    // `log(..)`
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);

    // The `console.log` is quite polymorphic, so we can bind it with multiple
    // signatures. Note that we need to use `js_name` to ensure we always call
    // `log` in JS.
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log_u32(a: u32);

    // Multiple arguments too!
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn log_many(a: &str, b: &str);
}
macro_rules! console_log {
    // Note that this is using the `log` function imported above during
    // `bare_bones`
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}
#[wasm_bindgen]
extern {
    fn alert(s: &str);
}

#[wasm_bindgen]
pub fn greet() {
    alert("Hello, wasm!");
}

#[wasm_bindgen]
#[derive(Debug,)]
pub struct WasmError(Error);


#[wasm_bindgen]
pub struct Backend {
    db_schema: DbSchema
}

fn cast_core_err(err: Error) -> JsError {
    let e = err.json_body();
    JsError::new(e.get("message").unwrap().as_str().unwrap_or("internal error"))
}

fn cast_serde_err(err: serde_json::Error) -> JsError {
    JsError::new(err.to_string().as_str())
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
        get: &Array, 
        body: Option<JsString>,
        headers: &Map,
        cookies: &Map,
    )
    -> Result<String, JsError> {
        let schema_name: String = schema_name.into();
        //console_log!("schema_name: {}", schema_name);
        let root: String = root.into();
        //console_log!("root: {}", root);
        let method: String = method.into();
        //console_log!("method: {}", method);
        let path: String = path.into();
        //console_log!("path: {}", path);
        let get:Vec<(String, String)> = get.into_serde().map_err(cast_serde_err)?;
        //console_log!("get: {:?}", get);
        let body:Option<String> = body.map(|s| s.into());
        //console_log!("body: {:?}", body);
        let headers: HashMap<String, String> = headers.into_serde().map_err(cast_serde_err)?;
        //console_log!("headers: {:?}", headers);
        let cookies: HashMap<String, String> = cookies.into_serde().map_err(cast_serde_err)?;
        //console_log!("cookies: {:?}", cookies);
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
        let request = parse(&schema_name, &root, db_schema, &method, path, get, body, headers, cookies, max_rows).map_err(cast_core_err)?;
        let query = fmt_main_query(&request.schema_name, &request).map_err(cast_core_err)?;
        let (main_statement, _main_parameters, _) = generate(query);
        Ok(main_statement)
    }
}
