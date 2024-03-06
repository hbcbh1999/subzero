#[macro_export]
macro_rules! check_null_ptr {
    ($ptr:expr, $msg:expr) => {
        if $ptr.is_null() {
            update_last_error(CoreError::InternalError { message: $msg.to_string() });
            return ptr::null_mut();
        }
    };
    ($ptr:expr, $ret:expr, $msg:expr) => {
        if $ptr.is_null() {
            update_last_error(CoreError::InternalError { message: $msg.to_string() });
            return $ret;
        }
    };
}

#[macro_export]
macro_rules! try_str_to_cstr {
    ($str:expr, $msg:literal) => {
        match CString::new($str) {
            Ok(cstr) => cstr,
            Err(_) => {
                update_last_error(CoreError::InternalError { message: $msg.to_string() });
                return ptr::null_mut();
            }
        }
    };
}

#[macro_export]
macro_rules! try_cstr_to_str {
    ($c_str:expr, $msg:literal) => {
        if $c_str.is_null() {
            update_last_error(CoreError::InternalError { message: $msg.to_string() });
            return ptr::null_mut();
        } else {
            let raw = CStr::from_ptr($c_str);
            match raw.to_str() {
                Ok(s) => s,
                Err(_) => {
                    update_last_error(CoreError::InternalError { message: $msg.to_string() });
                    return ptr::null_mut();
                }
            }
        }
    };
}

use std::ffi::CStr;
use std::collections::HashMap;

pub fn extract_cookies(cookie_header: Option<&str>) -> HashMap<&str, &str> {
    let mut cookies_map = HashMap::new();

    // Look for the "Cookie" header and parse its value if found
    if let Some(cookies) = cookie_header {
        // Cookies are typically separated by "; "
        for cookie in cookies.split("; ") {
            let parts: Vec<&str> = cookie.splitn(2, '=').collect();
            if parts.len() == 2 {
                let key = parts[0].trim();
                let value = parts[1].trim();
                cookies_map.insert(key, value);
            }
        }
    }

    cookies_map
}

use std::slice;
use crate::ffi::sbz_Tuple;
// Function to convert an array of Tuple structs to Vec<(&str, &str)>
pub fn tuples_to_vec<'a>(tuples_ptr: *const sbz_Tuple, length: usize) -> Result<Vec<(&'a str, &'a str)>, &'a str> {
    if tuples_ptr.is_null() {
        return Err("Null pointer passed as tuples");
    }

    // SAFETY: This block is safe if `tuples_ptr` points to a valid array of `Tuple` structs
    // of size `length`, and if each `key` and `value` in the array are valid pointers
    // to null-terminated C strings.
    let tuples_slice = unsafe { slice::from_raw_parts(tuples_ptr, length) };

    tuples_slice
        .iter()
        .map(|tuple| {
            let key_cstr = unsafe { CStr::from_ptr(tuple.key) };
            let value_cstr = unsafe { CStr::from_ptr(tuple.value) };

            let key_str = key_cstr.to_str().map_err(|_| "Invalid UTF-8 in key")?;
            let value_str = value_cstr.to_str().map_err(|_| "Invalid UTF-8 in value")?;

            Ok((key_str, value_str))
        })
        .collect()
}

use subzero_core::formatter::{ToParam, Param};
use subzero_core::api::{/*ListVal, */ SingleVal, Payload};
use std::borrow::Cow;
pub fn parameters_to_tuples<'a>(db_type: &'a str, parameters: Vec<&'a (dyn ToParam + Sync)>)
    -> Vec<(Cow<'a, str>, Cow<'a, str>)> {
    parameters
        .iter()
        .map(|p| {
            let param = match p.to_param() {
                Param::SV(SingleVal(v, Some(Cow::Borrowed("integer")))) => Cow::Borrowed(v.as_ref()),
                Param::SV(SingleVal(v, _)) => Cow::Borrowed(v.as_ref()),
                Param::PL(Payload(v, _)) => Cow::Borrowed(v.as_ref()),
                Param::Str(v) => Cow::Borrowed(v),
                Param::StrOwned(v) => Cow::Borrowed(v.as_str()),
                Param::LV(ListVal(v, _)) => match db_type {
                    "sqlite" | "mysql" => Cow::Owned(serde_json::to_string(v).unwrap_or_default()),
                    _ => Cow::Owned(format!("'{{{}}}'", v.join(", "))),
                }
            };
            let data_type:Cow<str> = match p.to_data_type(){
                Some(dt) => Cow::Borrowed(dt.as_ref()),
                None => Cow::Borrowed("unknown")
            };
            // (
            //     CString::new(param).unwrap().into_raw() as *const c_char,
            //     CString::new(data_type.as_ref().unwrap_or(&Cow::Borrowed("unknown")).as_ref()).unwrap().into_raw() as *const c_char
            // )
            (param, data_type)
        })
        .collect::<Vec<_>>()
}


use subzero_core::{
    schema::DbSchema as CoreDbSchema,
    
    api::{
        SelectItem,
        ListVal, Query, ApiRequest, QueryNode::*, Field, Condition, Filter},
    error::Error as CoreError,
};
// #[cfg(feature = "postgresql")]
// use subzero_core::formatter::postgresql;
// #[cfg(feature = "clickhouse")]
// use subzero_core::formatter::clickhouse;
#[cfg(feature = "sqlite")]
use subzero_core::formatter::sqlite;
#[cfg(feature = "mysql")]
use subzero_core::formatter::mysql;

pub fn fmt_first_stage_mutate<'a>(
    db_type: &'a str,
    db_schema: &'a CoreDbSchema,
    original_request: &'a ApiRequest,
    env: &'a HashMap<&str, &str>
) -> Result<(String, Vec<(String, String)>), CoreError> {

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
            let schema_obj = db_schema.get_object(original_request.schema_name, table)?;
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

    let (main_statement, main_parameters, _) = match db_type {
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            let query = sqlite::fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, env)?;
            Ok(sqlite::generate(query))
        }
        #[cfg(feature = "mysql")]
        "mysql" => {
            let query = mysql::fmt_main_query_internal(db_schema, schema_name, method, &accept_content_type, &query, &preferences, env)?;
            Ok(mysql::generate(query))
        }
        _ => Err(
            CoreError::InternalError {
                message: "unsupported database type for two step mutation".to_string(),
            }
        )
    }?;

    let pp = parameters_to_tuples(db_type, main_parameters)
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    Ok((main_statement, pp))
}

pub fn fmt_second_stage_select<'a>(
    db_type: &'a str,
    db_schema: &'a CoreDbSchema,
    original_request: &'a ApiRequest,
    env: &'a HashMap<&str, &str>
) -> Result<(String, Vec<(String, String)>), CoreError> {
    
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
            let schema_obj = db_schema.get_object(original_request.schema_name, table)?;
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
        _ => return Err(
            CoreError::InternalError {
                message: "unsupported database type for two step mutation".to_string(),
            }
        ),
    }

    let (main_statement, main_parameters, _) = match db_type {
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
            )?;
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
            )?;
            Ok(mysql::generate(query))
        }
        _ => Err(CoreError::InternalError {
            message: "unsupported database type for two step mutation".to_string()}),
    }?;
    let pp = parameters_to_tuples(db_type, main_parameters)
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    Ok((main_statement, pp))
}

// convert parameters vector to a js array
// fn parameters_to_js_array(db_type: &str, rust_parameters: Vec<&(dyn ToParam + Sync)>) -> JsArray {
//     let parameters = JsArray::new_with_length(rust_parameters.len() as u32);
//     for (i, p) in rust_parameters.into_iter().enumerate() {
//         let v = match p.to_param() {
//             LV(ListVal(v, _)) => match db_type {
//                 "sqlite" | "mysql" => to_js_value(&serde_json::to_string(v).unwrap_or_default()).unwrap_or_default(),
//                 _ => to_js_value(v).unwrap_or_default(),
//             },
//             SV(SingleVal(v, Some(Cow::Borrowed("integer")))) => to_js_value(&(v.parse::<i32>().unwrap_or_default())).unwrap_or_default(),
//             SV(SingleVal(v, _)) => to_js_value(v).unwrap_or_default(),
//             PL(Payload(v, _)) => to_js_value(v).unwrap_or_default(),
//             Str(v) => to_js_value(v).unwrap_or_default(),
//             StrOwned(v) => to_js_value(v).unwrap_or_default(),
//         };
//         parameters.set(i as u32, v);
//     }
//     //to_js_value(&parameters).unwrap_or_default()
//     parameters
// }
