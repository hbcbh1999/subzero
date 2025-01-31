// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
use js_sys::Array as JsArray;
use subzero_core::api::ApiRequest;
use wasm_bindgen::closure::Closure;
// use std::thread;
// use std::time::Duration;
use wasm_bindgen::{JsValue, prelude::wasm_bindgen};
use ouroboros::self_referencing;
use serde_wasm_bindgen::from_value as from_js_value;
use serde_wasm_bindgen::to_value as to_js_value;
use serde_wasm_bindgen::Error as JsError;
use std::borrow::Cow;
use std::collections::HashMap;
use serde_json::Value as JsonValue;
use std::sync::atomic::{AtomicBool, Ordering};

mod utils;
use utils::{set_panic_hook, cast_core_err, cast_serde_err, print_error_with_json_snippet, js_warn, console_warn, setTimeout, date_now};
use subzero_core::{
    parser::postgrest::parse,
    schema::{DbSchema, replace_json_str},
    formatter::{Param::*, ToParam},
    api::{SingleVal, ListVal, Payload, Query, Field, QueryNode::*, SelectItem, Condition, Filter, DEFAULT_SAFE_SELECT_FUNCTIONS},
    permissions::{check_privileges, check_safe_functions, insert_policy_conditions, replace_select_star},
    error::Error as CoreError,
    license::get_license_info,
};
#[cfg(feature = "postgresql")]
use subzero_core::formatter::postgresql;
#[cfg(feature = "clickhouse")]
use subzero_core::formatter::clickhouse;
#[cfg(feature = "sqlite")]
use subzero_core::formatter::sqlite;
#[cfg(feature = "mysql")]
use subzero_core::formatter::mysql;

// When the `wee_alloc` feature is enabled, use `wee_alloc` as the global
// allocator.
#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;
static DISABLED: AtomicBool = AtomicBool::new(false);
static PUBLIC_LICENSE_PEM: &str = include_str!("../../ecdsa_p256_public.pem");

struct BackendData {
    db_schema: String,
    db_type: String,
    allowed_select_functions: Vec<String>,
}

#[wasm_bindgen]
#[self_referencing]
pub struct Backend {
    data: Box<BackendData>,
    #[covariant]
    #[borrows(data)]
    inner: B<'this>,
}

pub struct B<'a> {
    db_schema: DbSchema<'a>,
    db_type: &'a str,
    allowed_select_functions: Vec<&'a str>,
}

#[wasm_bindgen]
impl Backend {
    pub fn init(db_schema: String, db_type: String, allowed_select_functions: JsValue, license_key: Option<String>) -> Result<Backend, JsError> {
        set_panic_hook();
        let license_data = match license_key {
            Some(k) => {
                let current_timestamp = (date_now() as i64) / 1000;
                match get_license_info(&k, PUBLIC_LICENSE_PEM, current_timestamp) {
                    Ok(l) => Some(l),
                    Err(e) => {
                        return Err(JsError::new(e));
                    }
                }
            }
            None => None,
        };
        if license_data.is_none() {
            console_warn!("subZero is running in demo mode. It will stop working after 15 minutes");
            // start a thread and set the DISABLED flag to true after 15 minutes
            let closure = Closure::new(|| {
                DISABLED.store(true, Ordering::Relaxed);
            });
            setTimeout(&closure, 900_000); // 15 minutes in milliseconds
        }

        let allowed_select_functions = from_js_value::<Option<Vec<String>>>(allowed_select_functions).unwrap_or_default();
        let allowed_select_functions = match allowed_select_functions {
            Some(v) => v,
            None => DEFAULT_SAFE_SELECT_FUNCTIONS.iter().map(|s| s.to_string()).collect(),
        };
        let mut s = db_schema;
        if db_type == "clickhouse" {
            //println!("json schema original:\n{:?}\n", s);
            // clickhouse query returns check_json_str and using_json_str as string
            // so we first parse it into a JsonValue and then convert those two fields into json
            let mut v: JsonValue = serde_json::from_str(&s).expect("invalid schema json");
            //recursively iterate through the json and convert check_json_str and using_json_str into json
            // println!("json value before replace:\n{:?}\n", v);
            // recursively iterate through the json and apply the f function
            replace_json_str(&mut v).expect("invalid schema json");
            s = serde_json::to_string_pretty(&v).expect("invalid schema json");
        }

        let backend = Backend::try_new(
            Box::new(BackendData {
                db_schema: s,
                db_type,
                allowed_select_functions,
            }),
            |data_ref| {
                let s = data_ref.db_schema.as_str();
                let b = B {
                    db_schema: serde_json::from_str(s)
                        .map_err(|err: serde_json::Error| print_error_with_json_snippet("invalid json schema", s, err))?,
                    db_type: data_ref.db_type.as_str(),
                    allowed_select_functions: data_ref.allowed_select_functions.iter().map(|s| s.as_str()).collect(),
                };
                Ok::<_, JsError>(b)
            },
        )?;

        Ok(backend)
        // Ok(Backend::new(
        //     Box::new(BackendData {
        //         db_schema: s,
        //         db_type,
        //         allowed_select_functions,
        //     }),
        //     |data_ref| B {
        //         db_schema: serde_json::from_str(data_ref.db_schema.as_str()).expect("invalid schema json"),
        //         db_type: data_ref.db_type.as_str(),
        //         allowed_select_functions: data_ref.allowed_select_functions.iter().map(|s| s.as_str()).collect(),
        //     },
        // ))
    }
    // pub fn set_schema(&mut self, s: &str){
    //     self.with_inner_mut(|inner| {
    //         inner.data.db_schema = s.to_owned();
    //         inner.db_schema = serde_json::from_str(inner.data.db_schema.as_str()).expect("invalid schema json");
    //     });
    // }
    #[allow(clippy::too_many_arguments)]
    fn parse<'a, 'b: 'a>(
        &'b self, schema_name: &'a str, root: &'a str, method: &'a str, path: &'a str, get: Vec<(&'a str, &'a str)>, body: &'a str, role: &'a str,
        headers: HashMap<&'a str, &'a str>, cookies: HashMap<&'a str, &'a str>, max_rows: Option<&'a str>,
    ) -> Result<ApiRequest<'a>, CoreError> {
        let backend = self.borrow_inner();
        let B {
            db_schema,
            allowed_select_functions,
            ..
        } = &backend;
        let body = if body.is_empty() { None } else { Some(body) };

        let mut request = parse(schema_name, root, db_schema, method, path, get, body, headers, cookies, max_rows)?;

        // replace "*" with the list of columns the user has access to
        // so that he does not encounter permission errors
        //log("request.query before replace_select_star");
        replace_select_star(db_schema, schema_name, role, &mut request.query)?;

        // in case when the role is not set (but authenticated through jwt) the query will be executed with the privileges
        // of the "authenticator" role unless the DbSchema has internal privileges set

        check_privileges(db_schema, schema_name, role, &request)?;
        check_safe_functions(&request, allowed_select_functions)?;
        insert_policy_conditions(db_schema, schema_name, role, &mut request.query)?;

        Ok(request)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fmt_main_query(
        &self, schema_name: String, root: String, method: String, path: String, get: JsValue, body: String, role: String, headers: JsValue,
        cookies: JsValue, env: JsValue, max_rows: Option<String>,
    ) -> Result<Vec<JsValue>, JsError> {
        if DISABLED.load(Ordering::Relaxed) {
            return Err(JsError::new("subZero is disabled"));
        }
        if !["GET", "POST", "PUT", "DELETE", "PATCH"].contains(&method.as_str()) {
            return Err(JsError::new("invalid method"));
        }

        let backend = self.borrow_inner();
        let B { db_schema, .. } = &backend;
        let db_type = backend.db_type;

        let get = from_js_value::<Vec<(String, String)>>(get).map_err(cast_serde_err("bad get param"))?;
        let get = get.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let headers = from_js_value::<Vec<(String, String)>>(headers).map_err(cast_serde_err("bad headers param"))?;
        let headers = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let cookies = from_js_value::<Vec<(String, String)>>(cookies).map_err(cast_serde_err("bad cookies param"))?;
        let cookies = cookies.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let env = from_js_value::<Vec<(String, String)>>(env).map_err(cast_serde_err("bad env param"))?;
        let env = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        let request = self
            .parse(&schema_name, &root, &method, &path, get, &body, &role, headers, cookies, max_rows.as_deref())
            .map_err(cast_core_err)?;

        let ApiRequest {
            method,
            schema_name,
            query,
            preferences,
            accept_content_type,
            ..
        } = request;
        let (main_statement, main_parameters, _) = match db_type {
            #[cfg(feature = "postgresql")]
            "postgresql" => {
                let query = postgresql::fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, &env)
                    .map_err(cast_core_err)?;
                Ok(postgresql::generate(query))
            }
            #[cfg(feature = "clickhouse")]
            "clickhouse" => {
                let query = clickhouse::fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, &env)
                    .map_err(cast_core_err)?;
                Ok(clickhouse::generate(query))
            }
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let query = sqlite::fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, &env)
                    .map_err(cast_core_err)?;
                Ok(sqlite::generate(query))
            }
            #[cfg(feature = "mysql")]
            "mysql" => {
                let query = mysql::fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, &env)
                    .map_err(cast_core_err)?;
                Ok(mysql::generate(query))
            }
            _ => Err(JsError::new("unsupported database type")),
        }?;

        Ok(vec![
            JsValue::from(main_statement),
            JsValue::from(parameters_to_js_array(db_type, main_parameters)),
        ])
    }

    fn fmt_first_stage_mutate(&self, original_request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<(JsValue, JsValue), JsError> {
        let backend = self.borrow_inner();
        let B { db_schema, db_type, .. } = backend;

        // create a clone of the request
        let mut request = original_request.clone();
        let is_delete = matches!(original_request.query.node, Delete { .. });

        // destructure the cloned request and eliminate the sub_selects and also select back
        match &mut request {
            ApiRequest {
                query:
                    Query {
                        sub_selects,
                        node:
                            Insert {
                                into: table,
                                returning,
                                select,
                                ..
                            },
                    },
                ..
            }
            | ApiRequest {
                query:
                    Query {
                        sub_selects,
                        node:
                            Delete {
                                from: table,
                                returning,
                                select,
                                ..
                            },
                    },
                ..
            }
            | ApiRequest {
                query:
                    Query {
                        sub_selects,
                        node: Update {
                            table, returning, select, ..
                        },
                    },
                ..
            } => {
                //sqlite does not support returning in CTEs so we must do a two step process
                //TODO!!! in rocket we dynamically generate the primary key column name
                let schema_obj = db_schema.get_object(original_request.schema_name, table).map_err(cast_core_err)?;
                let primary_key_column = schema_obj
                    .columns
                    .iter()
                    .find(|&(_, c)| c.primary_key)
                    .map(|(_, c)| c.name)
                    .unwrap_or("rowid");
                //let primary_key_column = "rowid"; //every table has this (TODO!!! check)
                let primary_key_field = Field {
                    name: primary_key_column,
                    json_path: None,
                };
                //return only the primary key column
                returning.clear();
                returning.push(primary_key_column);
                select.clear();
                select.push(SelectItem::Simple {
                    field: primary_key_field,
                    alias: Some(primary_key_column),
                    cast: None,
                });

                if !is_delete {
                    select.push(SelectItem::Simple {
                        field: Field {
                            name: "_subzero_check__constraint",
                            json_path: None,
                        },
                        alias: None,
                        cast: None,
                    });
                }
                // no need for additional data from joined tables
                sub_selects.clear();
            }
            _ => {}
        }

        let ApiRequest {
            method,
            schema_name,
            query,
            preferences,
            accept_content_type,
            ..
        } = request;

        let (main_statement, main_parameters, _) = match *db_type {
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let query = sqlite::fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, env)
                    .map_err(cast_core_err)?;
                Ok(sqlite::generate(query))
            }
            #[cfg(feature = "mysql")]
            "mysql" => {
                let query = mysql::fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, env)
                    .map_err(cast_core_err)?;
                Ok(mysql::generate(query))
            }
            _ => Err(JsError::new("unsupported database type for two step mutation")),
        }?;

        Ok((JsValue::from(main_statement), JsValue::from(parameters_to_js_array(db_type, main_parameters))))
    }

    fn fmt_second_stage_select(&self, original_request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<(JsValue, JsValue), JsError> {
        let backend = self.borrow_inner();
        let B { db_schema, db_type, .. } = backend;
        let ids: Vec<String> = vec!["_subzero_ids_placeholder_".to_string()];

        // create a clone of the request
        let mut request = original_request.clone();

        // destructure the cloned request and add the ids condition to the where clause
        // and make it a select query
        match &request.query {
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
                let schema_obj = db_schema.get_object(original_request.schema_name, table).map_err(cast_core_err)?;
                let primary_key_column = schema_obj
                    .columns
                    .iter()
                    .find(|&(_, c)| c.primary_key)
                    .map(|(_, c)| c.name)
                    .unwrap_or("rowid");
                //let primary_key_column = "rowid"; //every table has this (TODO!!! check)
                let primary_key_field = Field {
                    name: primary_key_column,
                    json_path: None,
                };

                let mut select_where = where_.to_owned();
                // add the primary key condition to the where clause
                select_where.conditions.insert(
                    0,
                    Condition::Single {
                        field: primary_key_field,
                        filter: Filter::In(ListVal(ids.into_iter().map(Cow::Owned).collect(), None)),
                        negate: false,
                    },
                );
                request.method = "GET";
                // set the request query to be a select
                request.query = Query {
                    node: Select {
                        check: None,
                        from: (table, Some("subzero_source")),
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

        let (main_statement, main_parameters, _) = match *db_type {
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let query = sqlite::fmt_main_query_internal(
                    db_schema,
                    request.schema_name,
                    request.method,
                    &request.accept_content_type,
                    &request.query,
                    &request.preferences,
                    env,
                )
                .map_err(cast_core_err)?;
                Ok(sqlite::generate(query))
            }
            #[cfg(feature = "mysql")]
            "mysql" => {
                let query = mysql::fmt_main_query_internal(
                    db_schema,
                    request.schema_name,
                    request.method,
                    &request.accept_content_type,
                    &request.query,
                    &request.preferences,
                    env,
                )
                .map_err(cast_core_err)?;
                Ok(mysql::generate(query))
            }
            _ => Err(JsError::new("unsupported database type")),
        }?;

        Ok((JsValue::from(main_statement), JsValue::from(parameters_to_js_array(db_type, main_parameters))))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fmt_sqlite_two_stage_query(
        &self, schema_name: String, root: String, method: String, path: String, get: JsValue, body: String, role: String, headers: JsValue,
        cookies: JsValue, env: JsValue, max_rows: Option<String>,
    ) -> Result<Vec<JsValue>, JsError> {
        self.fmt_two_stage_query(schema_name, root, method, path, get, body, role, headers, cookies, env, max_rows)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn fmt_two_stage_query(
        &self, schema_name: String, root: String, method: String, path: String, get: JsValue, body: String, role: String, headers: JsValue,
        cookies: JsValue, env: JsValue, max_rows: Option<String>,
    ) -> Result<Vec<JsValue>, JsError> {
        if DISABLED.load(Ordering::Relaxed) {
            return Err(JsError::new("subZero is disabled"));
        }
        if !["GET", "POST", "PUT", "DELETE", "PATCH"].contains(&method.as_str()) {
            return Err(JsError::new("invalid method"));
        }

        let backend = self.borrow_inner();
        let &B { db_type, .. } = backend;
        // check if backend is sqlite or mysql
        if !["sqlite", "mysql"].contains(&db_type) {
            return Err(JsError::new("fmt_two_stage_query is only supported for sqlite/mysql backend"));
        }

        let get = from_js_value::<Vec<(String, String)>>(get).map_err(cast_serde_err("bad get param"))?;
        let get = get.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let headers = from_js_value::<Vec<(String, String)>>(headers).map_err(cast_serde_err("bad headers param"))?;
        let headers = headers.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let cookies = from_js_value::<Vec<(String, String)>>(cookies).map_err(cast_serde_err("bad cookies param"))?;
        let cookies = cookies.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let env = from_js_value::<Vec<(String, String)>>(env).map_err(cast_serde_err("bad env param"))?;
        let env = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        let request = self
            .parse(&schema_name, &root, &method, &path, get, &body, &role, headers, cookies, max_rows.as_deref())
            .map_err(cast_core_err)?;
        let (mutate_statement, mutate_parameters) = self.fmt_first_stage_mutate(&request, &env)?;
        let (select_statement, select_parameters) = self.fmt_second_stage_select(&request, &env)?;

        Ok(vec![mutate_statement, mutate_parameters, select_statement, select_parameters])
    }
}

// convert parameters vector to a js array
fn parameters_to_js_array(db_type: &str, rust_parameters: Vec<&(dyn ToParam + Sync)>) -> JsArray {
    let parameters = JsArray::new_with_length(rust_parameters.len() as u32);
    for (i, p) in rust_parameters.into_iter().enumerate() {
        let v = match p.to_param() {
            LV(ListVal(v, _)) => match db_type {
                "sqlite" | "mysql" => to_js_value(&serde_json::to_string(v).unwrap_or_default()).unwrap_or_default(),
                _ => to_js_value(v).unwrap_or_default(),
            },
            SV(SingleVal(v, Some(Cow::Borrowed("integer")))) => to_js_value(&(v.parse::<i32>().unwrap_or_default())).unwrap_or_default(),
            SV(SingleVal(v, _)) => to_js_value(v).unwrap_or_default(),
            PL(Payload(v, _)) => to_js_value(v).unwrap_or_default(),
            Str(v) => to_js_value(v).unwrap_or_default(),
            StrOwned(v) => to_js_value(v).unwrap_or_default(),
        };
        parameters.set(i as u32, v);
    }
    //to_js_value(&parameters).unwrap_or_default()
    parameters
}
