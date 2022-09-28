// #![no_std]

mod utils;

use utils::{
    set_panic_hook, 
    cast_core_err, cast_serde_err,
    //console_log, log,
};
//use subzero_core::schema::DbSchema;
use subzero_core::{
    parser::postgrest::parse,
    schema::DbSchema,
    formatter::Param::*,
    api::{SingleVal, ListVal, Payload, ContentType, Query, Preferences}
};
#[cfg(feature = "postgresql")]
use subzero_core::formatter::postgresql;
#[cfg(feature = "clickhouse")]
use subzero_core::formatter::clickhouse;
#[cfg(feature = "sqlite")]
use subzero_core::formatter::sqlite;
use wasm_bindgen::{prelude::*, };
use js_sys::{Error as JsError, Array as JsArray,};
use serde_json;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
pub struct Request {
    schema_name: String,
    method: String,
    query: Query,
    preferences: Option<Preferences>,
    accept_content_type: ContentType,
}

#[wasm_bindgen]
pub struct Backend {
    db_schema: DbSchema,
    db_type: String,
}

// #[wasm_bindgen]
// pub struct Statement {
//     pub statement: String,
//     pub parameters: Vec<JsValue>,
// }

#[wasm_bindgen]
impl Backend {
    pub fn init(s: &str, dt: &str) -> Backend {
        set_panic_hook();
        let db_schema = serde_json::from_str(s).expect("invalid schema json");
        let db_type = dt.to_owned();
        Backend { db_schema, db_type }
    }
    pub fn parse(
        &self,
        schema_name: &str,
        root: &str, 
        method: &str, 
        path: &str, 
        get: &JsArray, 
        body: &str,
        headers: &JsArray,
        cookies: &JsArray,
    )
    -> Result<Request, JsError>
    {
        if !["GET","POST","PUT","DELETE","PATCH"].contains(&method) {
            return Err(JsError::new("invalid method"));
        }
        
        let get = get.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
        let get = get.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
        let headers = headers.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
        let headers = headers.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
        let cookies = cookies.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
        let cookies = cookies.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
        let db_schema = &self.db_schema;
        let body = if body.is_empty() {
            None
        } else {
            Some(body)
        };
        let max_rows = None;

        let rust_request = parse(schema_name, root, db_schema, method, path, get, body, headers, cookies, max_rows).map_err(cast_core_err)?;

        Ok(Request {
            method: rust_request.method.to_string(),
            schema_name: rust_request.schema_name.to_string(),
            query: rust_request.query,
            preferences: rust_request.preferences,
            accept_content_type: rust_request.accept_content_type,
        })
        

    }

    pub fn fmt_main_query(&self, request: &Request, env: &JsArray) -> Result<Vec<JsValue>, JsError> {
        
        let db_type = self.db_type.as_str();
        let env = env.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
        let env = env.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
        let (main_statement, main_parameters, _) = match db_type {
            #[cfg(feature = "postgresql")]
            "postgresql" => {
                let query = postgresql::fmt_main_query_internal(request.schema_name.as_str(), &request.method, &request.accept_content_type, &request.query, &request.preferences, &env).map_err(cast_core_err)?;
                Ok(postgresql::generate(query))
            },
            #[cfg(feature = "clickhouse")]
            "clickhouse" => {
                let query = clickhouse::fmt_main_query_internal(request.schema_name.as_str(), &request.method, &request.accept_content_type, &request.query, &request.preferences, &env).map_err(cast_core_err)?;
                Ok(clickhouse::generate(query))
            },
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let query = sqlite::fmt_main_query_internal(request.schema_name.as_str(), &request.method, &request.accept_content_type, &request.query, &request.preferences, &env).map_err(cast_core_err)?;
                Ok(sqlite::generate(query))
            },
            _ => Err(JsError::new("unsupported database type")),
        }?;
        // let parameters = main_parameters.into_iter().map(|p| {
        //     match p.to_param() {
        //         LV(ListVal(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
        //         SV(SingleVal(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
        //         PL(Payload(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
        //         TV(v) => JsValue::from_serde(v).unwrap_or_default(),
        //     }
        // }).collect::<Vec<_>>();

        // Ok(Statement {
        //     statement: main_statement,
        //     parameters,
        // })
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

    // pub fn get_query(
    //     &self,
    //     schema_name: &str,
    //     root: &str, 
    //     method: &str, 
    //     path: &str, 
    //     get: &JsArray, 
    //     //body: Option<JsString>,
    //     body: &str,
    //     headers: &JsArray,
    //     cookies: &JsArray,
    //     env: &JsArray,
    //     //db_type: Option<JsString>,
    //     db_type: &str,
    //     return_core_query: bool,
    // )
    // -> Result<Vec<JsValue>, JsError> {
        
    //     if !["GET","POST","PUT","DELETE","PATCH"].contains(&method) {
    //         return Err(JsError::new("invalid method"));
    //     }
        
    //     let get = get.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
    //     let get = get.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
    //     let headers = headers.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
    //     let headers = headers.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
    //     let cookies = cookies.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
    //     let cookies = cookies.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
    //     let env = env.into_serde::<Vec<(String,String)>>().map_err(cast_serde_err)?;
    //     let env = env.iter().map(|(k,v)|(k.as_str(),v.as_str())).collect();
    //     let db_schema = &self.db_schema;
    //     let body = if body.is_empty() {
    //         None
    //     } else {
    //         Some(body)
    //     };
    //     let max_rows = None;

    //     let request = parse(schema_name, root, db_schema, method, path, get, body, headers, cookies, max_rows).map_err(cast_core_err)?;

    //     let (main_statement, main_parameters, _) = match db_type {
    //         #[cfg(feature = "postgresql")]
    //         "postgresql" => {
    //             let query = if !return_core_query
    //                         {postgresql::fmt_main_query(request.schema_name, &request, &env).map_err(cast_core_err)?}
    //                         else
    //                         {postgresql::fmt_query(&request.schema_name.to_string(), false, None, &request.query,&None).map_err(cast_core_err)?};
    //             Ok(postgresql::generate(query))
    //         },
    //         #[cfg(feature = "clickhouse")]
    //         "clickhouse" => {
    //             let query = if !return_core_query
    //                         {clickhouse::fmt_main_query(request.schema_name, &request, &env).map_err(cast_core_err)?}
    //                         else
    //                         {clickhouse::fmt_query(&request.schema_name.to_string(), false, None, &request.query,&None).map_err(cast_core_err)?};
    //             Ok(clickhouse::generate(query))
    //         },
    //         #[cfg(feature = "sqlite")]
    //         "sqlite" => {
    //             let query = if !return_core_query
    //                         {sqlite::fmt_main_query(request.schema_name, &request, &env).map_err(cast_core_err)?}
    //                         else
    //                         {sqlite::fmt_query(&request.schema_name.to_string(), false, None, &request.query,&None).map_err(cast_core_err)?};
    //             Ok(sqlite::generate(query))
    //         },
    //         _ => Err(JsError::new("unsupported database type")),
    //     }?;
    //     let parameters = JsArray::new_with_length(main_parameters.len() as u32);
    //     for (i, p) in main_parameters.into_iter().enumerate() {
    //         let v = match p.to_param() {
    //             LV(ListVal(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
    //             SV(SingleVal(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
    //             PL(Payload(v,_)) => JsValue::from_serde(v).unwrap_or_default(),
    //             TV(v) => JsValue::from_serde(v).unwrap_or_default(),
    //         };
    //         parameters.set(i as u32, v);
    //     }
    //     Ok(vec![JsValue::from(main_statement), JsValue::from(parameters)])
    // }
}


