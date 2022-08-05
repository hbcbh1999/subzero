// #![no_std]

mod utils;
use std::collections::HashMap;

use utils::{
    set_panic_hook, 
    cast_core_err, cast_serde_err,
    // console_log, log,
};
//use subzero_core::schema::DbSchema;
use subzero_core::{
    parser::postgrest::parse,
    schema::DbSchema,
    formatter::Param::*,
    api::{SingleVal, ListVal, Payload}
};
#[cfg(feature = "postgresql")]
use subzero_core::formatter::postgresql;
#[cfg(feature = "clickhouse")]
use subzero_core::formatter::clickhouse;
#[cfg(feature = "sqlite")]
use subzero_core::formatter::sqlite;
use wasm_bindgen::{prelude::*, };
use js_sys::{Error as JsError, Array as JsArray, Map as JsMap};
use serde_json;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
pub struct Backend {
    db_schema: DbSchema
}

#[wasm_bindgen]
impl Backend {
    pub fn init(s: &str) -> Backend {
        set_panic_hook();
        let db_schema = serde_json::from_str(s).expect("invalid schema json");
        Backend { db_schema }
    }
    pub fn get_query(
        &self,
        schema_name: &str,
        root: &str, 
        method: &str, 
        path: &str, 
        get: &JsArray, 
        //body: Option<JsString>,
        body: &str,
        headers: &JsMap,
        cookies: &JsMap,
        env: &JsMap,
        //db_type: Option<JsString>,
        db_type: &str,
    )
    -> Result<Vec<JsValue>, JsError> {
        
        if !["GET","POST","PUT","DELETE","PATCH"].contains(&method) {
            return Err(JsError::new("invalid method"));
        }
        //let body = body.into();
        let get = get.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
        let get = get.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
        let headers = headers.into_serde::<HashMap<String,String>>().map_err(cast_serde_err)?;
        let headers = headers.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
        let cookies = cookies.into_serde::<HashMap<String,String>>().map_err(cast_serde_err)?;
        let cookies = cookies.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
        let env = env.into_serde::<HashMap<String,String>>().map_err(cast_serde_err)?;
        let env = env.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
        let db_schema = &self.db_schema;
        
        let body = if body.is_empty() {
            None
        } else {
            Some(body)
        };
        let max_rows = None;

        let request = parse(schema_name, root, db_schema, method, path, get, body, headers, cookies, max_rows).map_err(cast_core_err)?;

        //let db_type = db_type.unwrap_or(JsString::from(""));
        let (main_statement, main_parameters, _) = match db_type {
            #[cfg(feature = "postgresql")]
            "postgresql" => {
                let query = postgresql::fmt_main_query(request.schema_name, &request, &env).map_err(cast_core_err)?;
                Ok(postgresql::generate(query))
            },
            #[cfg(feature = "clickhouse")]
            "clickhouse" => {
                let query = clickhouse::fmt_main_query(request.schema_name, &request, &env).map_err(cast_core_err)?;
                Ok(clickhouse::generate(query))
            },
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let query = sqlite::fmt_main_query(request.schema_name, &request, &env).map_err(cast_core_err)?;
                Ok(sqlite::generate(query))
            },
            _ => Err(JsError::new("unsupported database type")),
        }?;
        let parameters = JsArray::new_with_length(main_parameters.len() as u32);
        for (i, p) in main_parameters.into_iter().enumerate() {
            let v = match p.to_param() {
                LV(ListVal(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
                SV(SingleVal(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
                PL(Payload(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
                TV(v) => JsValue::from_serde(v).unwrap_or_default(),
            };
            parameters.set(i as u32, v);
        }
        Ok(vec![JsValue::from(main_statement), JsValue::from(parameters)])
    }
}


