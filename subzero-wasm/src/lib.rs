use serde::{Serialize, Deserialize};
use js_sys::{Array as JsArray};
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};
use serde_wasm_bindgen::from_value as from_js_value;
use serde_wasm_bindgen::to_value as to_js_value;
use serde_wasm_bindgen::Error as JsError;
mod utils;
use utils::{
    set_panic_hook,
    cast_core_err,
    cast_serde_err,
    //console_log, log,
};
use subzero_core::{
    parser::postgrest::parse,
    schema::DbSchema,
    formatter::{Param::*, ToParam},
    api::{
        SingleVal, ListVal, Payload, ContentType, Query, Preferences, Field, QueryNode::*, SelectItem, Condition, Filter,
        DEFAULT_SAFE_SELECT_FUNCTIONS,
    },
    permissions::{check_privileges, check_safe_functions, insert_policy_conditions},
};
#[cfg(feature = "postgresql")]
use subzero_core::formatter::postgresql;
#[cfg(feature = "clickhouse")]
use subzero_core::formatter::clickhouse;
#[cfg(feature = "sqlite")]
use subzero_core::formatter::sqlite;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen]
#[derive(Serialize, Deserialize, Clone)]
pub struct Request {
    schema_name: String,
    method: String,
    query: Query,
    preferences: Option<Preferences>,
    accept_content_type: ContentType,
}

#[wasm_bindgen]
#[derive(Serialize, Deserialize)]
pub struct Backend {
    db_schema: DbSchema,
    db_type: String,
    allowed_select_functions: Vec<String>,
}

#[wasm_bindgen]
impl Backend {
    pub fn init(s: &str, dt: &str, allowed_select_functions: JsValue) -> Backend {
        set_panic_hook();
        let db_schema = serde_json::from_str(s).expect("invalid schema json");
        let db_type = dt.to_owned();
        let allowed_select_functions = from_js_value::<Option<Vec<String>>>(allowed_select_functions).unwrap_or_default();
        let allowed_select_functions = match allowed_select_functions {
            Some(v) => v,
            None => DEFAULT_SAFE_SELECT_FUNCTIONS.iter().map(|s| s.to_string()).collect(),
        };
        Backend {
            db_schema,
            db_type,
            allowed_select_functions,
        }
    }
    pub fn set_schema(&mut self, s: &str) {
        let schema = serde_json::from_str(s).expect("invalid schema json");
        self.db_schema = schema
    }
    #[allow(clippy::too_many_arguments)]
    pub fn parse(
        &self, schema_name: &str, root: &str, method: &str, path: &str, get: JsValue, body: &str, role: &str, headers: JsValue, cookies: JsValue,
        max_rows: Option<u32>,
    ) -> Result<Request, JsError> {
        if !["GET", "POST", "PUT", "DELETE", "PATCH"].contains(&method) {
            return Err(JsError::new("invalid method"));
        }

        let get = from_js_value::<Vec<(String, String)>>(get).map_err(cast_serde_err)?;
        let get = get.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let headers = from_js_value::<Vec<(String, String)>>(headers).map_err(cast_serde_err)?;
        let headers = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let cookies = from_js_value::<Vec<(String, String)>>(cookies).map_err(cast_serde_err)?;
        let cookies = cookies.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let db_schema = &self.db_schema;
        let schema_name_string = schema_name.to_owned();
        let role = role.to_owned();
        let body = if body.is_empty() { None } else { Some(body) };
        //let max_rows = None;

        let mut rust_request = parse(schema_name, root, db_schema, method, path, get, body, headers, cookies, max_rows).map_err(cast_core_err)?;
        // in case when the role is not set (but authenticated through jwt) the query will be executed with the privileges
        // of the "authenticator" role unless the DbSchema has internal privileges set

        check_privileges(db_schema, &schema_name_string, &role, &rust_request).map_err(cast_core_err)?;
        check_safe_functions(&rust_request, &self.allowed_select_functions).map_err(cast_core_err)?;
        insert_policy_conditions(db_schema, &schema_name_string, &role, &mut rust_request.query).map_err(cast_core_err)?;

        Ok(Request {
            method: rust_request.method.to_string(),
            schema_name: rust_request.schema_name.to_string(),
            query: rust_request.query,
            preferences: rust_request.preferences,
            accept_content_type: rust_request.accept_content_type,
        })
    }

    pub fn fmt_main_query(&self, request: &Request, env: JsValue) -> Result<Vec<JsValue>, JsError> {
        let db_type = self.db_type.as_str();
        let env = from_js_value::<Vec<(String, String)>>(env).map_err(cast_serde_err)?;
        let env = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let (main_statement, main_parameters, _) = match db_type {
            #[cfg(feature = "postgresql")]
            "postgresql" => {
                let query = postgresql::fmt_main_query_internal(
                    request.schema_name.as_str(),
                    &request.method,
                    &request.accept_content_type,
                    &request.query,
                    &request.preferences,
                    &env,
                )
                .map_err(cast_core_err)?;
                Ok(postgresql::generate(query))
            }
            #[cfg(feature = "clickhouse")]
            "clickhouse" => {
                let query = clickhouse::fmt_main_query_internal(
                    request.schema_name.as_str(),
                    &request.method,
                    &request.accept_content_type,
                    &request.query,
                    &request.preferences,
                    &env,
                )
                .map_err(cast_core_err)?;
                Ok(clickhouse::generate(query))
            }
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let query = sqlite::fmt_main_query_internal(
                    request.schema_name.as_str(),
                    &request.method,
                    &request.accept_content_type,
                    &request.query,
                    &request.preferences,
                    &env,
                )
                .map_err(cast_core_err)?;
                Ok(sqlite::generate(query))
            }
            _ => Err(JsError::new("unsupported database type")),
        }?;
        Ok(vec![JsValue::from(main_statement), JsValue::from(parameters_to_js_array(main_parameters))])
    }

    pub fn fmt_sqlite_mutate_query(&self, original_request: &Request, env: JsValue) -> Result<Vec<JsValue>, JsError> {
        let db_type = self.db_type.as_str();
        let env = from_js_value::<Vec<(String, String)>>(env).map_err(cast_serde_err)?;
        let env = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        //sqlite does not support returining in CTEs so we must do a two step process
        let primary_key_column = "rowid"; //every table has this (TODO!!! check)
        let primary_key_field = Field {
            name: primary_key_column.to_string(),
            json_path: None,
        };

        // create a clone of the request
        let mut request: Request = original_request.clone();
        let is_delete = matches!(request.query.node, Delete { .. });

        // eliminate the sub_selects and also select back
        match &mut request {
            Request {
                query: Query {
                    sub_selects,
                    node: Insert { returning, select, .. },
                },
                ..
            }
            | Request {
                query: Query {
                    sub_selects,
                    node: Delete { returning, select, .. },
                },
                ..
            }
            | Request {
                query: Query {
                    sub_selects,
                    node: Update { returning, select, .. },
                },
                ..
            } => {
                //return only the primary key column
                returning.clear();
                returning.push(primary_key_column.to_string());
                select.clear();
                select.push(SelectItem::Simple {
                    field: primary_key_field,
                    alias: Some(primary_key_column.to_string()),
                    cast: None,
                });

                if !is_delete {
                    select.push(SelectItem::Simple {
                        field: Field {
                            name: "_subzero_check__constraint".to_string(),
                            json_path: None,
                        },
                        alias: None,
                        cast: None,
                    });
                }
                // no need for aditional data from joined tables
                sub_selects.clear();
            }
            _ => {}
        }

        let (main_statement, main_parameters, _) = match db_type {
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let query = sqlite::fmt_main_query_internal(
                    request.schema_name.as_str(),
                    &request.method,
                    &request.accept_content_type,
                    &request.query,
                    &request.preferences,
                    &env,
                )
                .map_err(cast_core_err)?;
                Ok(sqlite::generate(query))
            }
            _ => Err(JsError::new("unsupported database type for two step mutation")),
        }?;

        Ok(vec![JsValue::from(main_statement), JsValue::from(parameters_to_js_array(main_parameters))])
    }

    pub fn fmt_sqlite_second_stage_select(&self, original_request: &Request, ids: JsValue, env: JsValue) -> Result<Vec<JsValue>, JsError> {
        let db_type = self.db_type.as_str();
        let env = from_js_value::<Vec<(String, String)>>(env).map_err(cast_serde_err)?;
        let env = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let ids = from_js_value::<Vec<String>>(ids).map_err(cast_serde_err)?;
        // create a clone of the request
        let mut request: Request = original_request.clone();
        match &original_request.query {
            Query {
                node: Insert {
                    into: table, where_, select, ..
                },
                sub_selects,
            }
            | Query {
                node: Update { table, where_, select, .. },
                sub_selects,
            }
            | Query {
                node: Delete {
                    from: table, where_, select, ..
                },
                sub_selects,
            } => {
                let primary_key_column = "rowid"; //every table has this (TODO!!! check)
                let primary_key_field = Field {
                    name: primary_key_column.to_string(),
                    json_path: None,
                };

                let mut select_where = where_.to_owned();
                // add the primary key condition to the where clause
                select_where.conditions.insert(
                    0,
                    Condition::Single {
                        field: primary_key_field,
                        filter: Filter::In(ListVal(ids, None)),
                        negate: false,
                    },
                );
                request.method = "GET".to_string();
                // set the request query to be a select
                request.query = Query {
                    node: Select {
                        from: (table.to_owned(), Some("subzero_source".to_string())),
                        join_tables: vec![], //todo!! this should probably not be empty
                        where_: select_where,
                        select: select.to_vec(),
                        limit: None,
                        offset: None,
                        order: vec![],
                        groupby: vec![],
                    },
                    sub_selects: sub_selects.to_vec(),
                };
            }
            _ => return Err(JsError::new("unsupported query type for two step mutation")),
        }

        let (main_statement, main_parameters, _) = match db_type {
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let query = sqlite::fmt_main_query_internal(
                    request.schema_name.as_str(),
                    &request.method,
                    &request.accept_content_type,
                    &request.query,
                    &request.preferences,
                    &env,
                )
                .map_err(cast_core_err)?;
                Ok(sqlite::generate(query))
            }
            _ => Err(JsError::new("unsupported database type")),
        }?;
        Ok(vec![JsValue::from(main_statement), JsValue::from(parameters_to_js_array(main_parameters))])
    }
}

// convert parameters vector to a js array
fn parameters_to_js_array(rust_parameters: Vec<&(dyn ToParam + Sync)>) -> JsArray {
    let parameters = JsArray::new_with_length(rust_parameters.len() as u32);
    for (i, p) in rust_parameters.into_iter().enumerate() {
        let v = match p.to_param() {
            LV(ListVal(v, _)) => to_js_value(&serde_json::to_string(v).unwrap_or_default()).unwrap_or_default(),
            SV(SingleVal(v, _)) => to_js_value(&v).unwrap_or_default(),
            PL(Payload(v, _)) => to_js_value(&v).unwrap_or_default(),
            TV(v) => to_js_value(&v).unwrap_or_default(),
        };
        parameters.set(i as u32, v);
    }
    //to_js_value(&parameters).unwrap_or_default()
    parameters
}
