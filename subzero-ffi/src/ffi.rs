#![allow(non_camel_case_types)]

use std::ffi::{CStr, CString};
use std::ptr;
use libc::{c_char, c_int};
// use std::collections::HashMap;
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::error::Error as StdError;
use std::slice;
use ouroboros::self_referencing;
use std::collections::HashMap;
use url::Url;
use subzero_core::{
    parser::postgrest::parse,
    schema::{DbSchema as CoreDbSchema, replace_json_str},
    
    api::ApiRequest,
    // permissions::{check_privileges, check_safe_functions, insert_policy_conditions, replace_select_star},
    error::Error as CoreError,
};
use crate::{check_null_ptr, try_cstr_to_str};
use crate::utils::{extract_cookies, tuples_to_vec, parameters_to_tuples,
    fmt_first_stage_mutate, fmt_second_stage_select
};
#[cfg(feature = "postgresql")]
use subzero_core::formatter::postgresql;
#[cfg(feature = "clickhouse")]
use subzero_core::formatter::clickhouse;
#[cfg(feature = "sqlite")]
use subzero_core::formatter::sqlite;
#[cfg(feature = "mysql")]
use subzero_core::formatter::mysql;

thread_local! {
    static LAST_ERROR: RefCell<Option<Box<dyn StdError>>> = RefCell::new(None);
}

/// A structure for holding the information about database entities and permissions (tables, views, etc).
#[self_referencing]
pub struct sbz_DbSchema {
    db_type: String,
    data: String,
    #[covariant]
    #[borrows(data)]
    inner: CoreDbSchema<'this>,
}

/// A structure representing a SQL statement (query and parameters).
#[derive(Debug)]
pub struct sbz_Statement {
    sql: CString,
    _params: Vec<(CString, CString)>,
    params_values: Vec<*const c_char>,
    params_types: Vec<*const c_char>,
}
impl sbz_Statement {
    pub fn new(sql: &str, params: Vec<(&str, &str)>) -> Self {
        let sql = CString::new(sql).unwrap();
        let _params: Vec<(CString, CString)> = params
            .into_iter()
            .map(|(key, value)| (CString::new(key).unwrap(), CString::new(value).unwrap()))
            .collect();

        let params_values: Vec<*const c_char> = _params.iter().map(|(value, _)| value.as_ptr()).collect();
        let params_types: Vec<*const c_char> = _params.iter().map(|(_, type_)| type_.as_ptr()).collect();

        sbz_Statement {
            sql,
            _params,
            params_values,
            params_types,
        }
    }
}

/// A structure representing a two-stage statement (mutate and select).
/// The mutate statement is used to perform the mutation (insert, update, delete)
/// and the select statement is used to retrieve the result.
/// This is used for databases that do not support returning the result of a mutation (sqlite/mysql).
/// 
/// Both statements should be executed in the same transaction.
/// The mutate statement will return two columns: id and _subzero_check__constraint
/// You need to collect the ids and then call two_stage_statement_set_ids to set the ids for the select statement
/// before calling `sbz_two_stage_statement_select`.
/// You also need to check the _subzero_check__constraint column to be "truethy" for all rows and rollback
/// the transaction if it is not.
#[derive(Debug)]
pub struct sbz_TwoStageStatement {
    db_type: String,
    mutate: sbz_Statement,
    select: sbz_Statement,
    ids_set: bool,
}
impl sbz_TwoStageStatement {
    pub fn new(db_type: String, mutate: sbz_Statement, select: sbz_Statement) -> Self {
        sbz_TwoStageStatement { db_type, mutate, select, ids_set: false }
    }
    pub fn set_ids(&mut self, ids: Vec<&str>) {
        let select_params = &self.select._params;
        // find the param equal to _subzero_ids_placeholder_ and replace it with the ids
        let placehoder_vec = vec!["_subzero_ids_placeholder_"];
        let placehoder = CString::new(match self.db_type.as_str() {
            "sqlite" | "mysql" => serde_json::to_string(&placehoder_vec).unwrap_or_default(),
            _ => format!("'{{{}}}'", placehoder_vec.join(", ")),
        }).unwrap();
        let mut pos = None;
        for (i, (val, _)) in select_params.iter().enumerate() {
            if val == &placehoder {
                pos = Some(i);
            }
        }
        let new_val = CString::new(match self.db_type.as_str() {
            "sqlite" | "mysql" => serde_json::to_string(&ids).unwrap_or_default(),
            _ => format!("'{{{}}}'", ids.join(", ")),
        }).unwrap();
        if let Some(pos) = pos {
            self.select._params[pos].0 = new_val;
            self.select.params_values[pos] = self.select._params[pos].0.as_ptr();
            self.ids_set = true;
        }
    }
}

/// A structure representing a key-value pair.
#[repr(C)]
pub struct sbz_Tuple {
    pub key: *const c_char,
    pub value: *const c_char,
}

/// A structure representing a HTTP request.
/// This is used to pass the information about the request to the subzero core.
/// 
/// # Fields
/// - `method` - The HTTP method (GET, POST, PUT, DELETE, etc).
/// - `uri` - The full URI of the request (including the query string, ex: http://example.com/path?query=string).
/// - `headers` - An array of key-value pairs representing the headers of the request.
/// - `headers_count` - The number of headers in the `headers` array.
/// - `body` - The body of the request (pass NULL if there is no body).
/// - `env` - An array of key-value pairs representing the environment data that needs to be available to the query.
/// - `env_count` - The number of key-value pairs in the `env` array.
#[repr(C)]
pub struct sbz_HTTPRequest {
    pub method: *const c_char,
    pub uri: *const c_char,
    pub headers: *const sbz_Tuple,
    pub headers_count: c_int,
    pub body: *const c_char,
    pub env: *const sbz_Tuple,
    pub env_count: c_int,
}

/// Create a new `sbz_TwoStageStatement`
/// # Safety
/// 
/// # Parameters
/// - `schema_name` - The name of the database schema for the current request (ex: public).
/// - `path_prefix` - The prefix of the path for the current request (ex: /api/).
/// - `db_schema` - A pointer to the `sbz_DbSchema`
/// - `request` - A pointer to the `sbz_HTTPRequest`
/// - `max_rows` - The maximum number of rows to return (pass NULL if there is no limit, otherwise pass a string representing the number of rows).
/// 
/// # Returns
/// A pointer to the newly created `sbz_TwoStageStatement` or a null pointer if an error occurred.
/// 
/// # Example
/// ```c
/// const char* db_type = "sqlite";
/// sbz_DbSchema* db_schema = sbz_db_schema_new(db_type, db_schema_json); // see db_schema_new example for db_schema_json
/// sbz_Tuple headers[] = {{"Content-Type", "application/json"}, {"Accept", "application/json"}};
/// sbz_Tuple env[] = {{"user_id", "1"}};
/// sbz_HTTPRequest req = {
///     "POST",
///     "http://localhost/rest/projects?select=id,name",
///     headers, 2,
///     "[{\"name\":\"project1\"}]", 
///     env, 1
/// };
/// sbz_TwoStageStatement* stmt = sbz_two_stage_statement_new(
///     "public",
///     "/rest/",
///     db_schema,
///     &req,
///     NULL
/// );
///
/// if (stmt == NULL) {
///     const int err_len = sbz_last_error_length();
///     char* err = (char*)malloc(err_len);
///     sbz_last_error_message(err, err_len);
///     printf("Error: %s\n", err);
///     free(err);
///     return;
/// }
/// const sbz_Statement* mutate_stmt = sbz_two_stage_statement_mutate(stmt);
/// const char* sql = sbz_statement_sql(mutate_stmt);
/// const char *const * params = sbz_statement_params(mutate_stmt);
/// const char *const * params_types = sbz_statement_params_types(mutate_stmt);
/// int params_count = sbz_statement_params_count(mutate_stmt);
/// printf("mutate SQL: %s\n", sql);
/// printf("mutate params: %s\n", params[0]);
/// printf("mutate params_count: %d\n", params_count);
/// 
/// // collect the ids from the result of the mutate statement
/// // and set them for the select statement
/// const char *ids[] = {"1", "2", "3"};
/// const int ids_set = sbz_two_stage_statement_set_ids(main_stmt, ids, 3);
///
/// const sbz_Statement* select_stmt = sbz_two_stage_statement_select(stmt);
/// const char* sql_select = sbz_statement_sql(select_stmt);
/// const char *const * params_select = sbz_statement_params(select_stmt);
/// const char *const * params_types_select = sbz_statement_params_types(select_stmt);
/// int params_count_select = sbz_statement_params_count(select_stmt);
/// printf("select SQL: %s\n", sql_select);
/// printf("select params: %s\n", params_select[0]);
/// printf("select params_count: %d\n", params_count_select);
/// 
/// // free the memory associated with the two_stage_statement
/// sbz_two_stage_statement_free(main_stmt);
#[no_mangle]
pub unsafe extern "C" fn sbz_two_stage_statement_new(
    schema_name: *const c_char,
    path_prefix: *const c_char,
    db_schema: *const sbz_DbSchema,
    request: *const sbz_HTTPRequest,
    max_rows: *const c_char,
) -> *mut sbz_TwoStageStatement {
    check_null_ptr!(db_schema, "Null pointer passed as the schema");
    let db_schema = unsafe { &*db_schema };
    check_null_ptr!(request, "Null pointer passed as the request");
    let request = unsafe { &*request };
    let schema_name_str = try_cstr_to_str!(schema_name, "Invalid UTF-8 in schema_name");
    let path_prefix_str = try_cstr_to_str!(path_prefix, "Invalid UTF-8 in path_prefix");
    let method_str = try_cstr_to_str!(request.method, "Invalid UTF-8 in method");
    let body_str = if request.body.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(request.body, "Invalid UTF-8 in body"))
    };
    let uri_str = try_cstr_to_str!(request.uri, "Invalid UTF-8 in uri");
    let parsed_uri = match Url::parse(uri_str) {
        Ok(u) => u,
        Err(e) => {
            update_last_error(CoreError::InternalError {
                message: format!("Unable to parse uri: {}", e),
            });
            return ptr::null_mut();
        }
    };
    let query_pairs: Vec<(String, String)> = parsed_uri
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    let get: Vec<(&str, &str)> = query_pairs.iter().map(|(key, value)| (key.as_ref(), value.as_ref())).collect();

    let headers = match tuples_to_vec(request.headers, request.headers_count as usize) {
        Ok(v) => HashMap::from_iter(v),
        Err(e) => {
            update_last_error(CoreError::InternalError { message: e.to_string() });
            return ptr::null_mut();
        }
    };
    let env = match tuples_to_vec(request.env, request.env_count as usize) {
        Ok(v) => HashMap::from_iter(v),
        Err(e) => {
            update_last_error(CoreError::InternalError { message: e.to_string() });
            return ptr::null_mut();
        }
    };
    let cookies = extract_cookies(headers.get("Cookie").copied());
    let max_rows_opt = if max_rows.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(max_rows, "Invalid UTF-8 in max_rows"))
    };

    let object_str = match parsed_uri.path().strip_prefix(path_prefix_str) {
        Some(s) => s,
        None => {
            update_last_error(CoreError::InternalError {
                message: format!("Unable to strip prefix: {}", path_prefix_str),
            });
            return ptr::null_mut();
        }
    };
    let api_request_result = parse(
        schema_name_str,
        object_str,
        db_schema.borrow_inner(),
        method_str,
        parsed_uri.path(),
        get,
        body_str,
        headers,
        cookies,
        max_rows_opt,
    );

    let api_request = match api_request_result {
        Ok(r) => r,
        Err(e) => {
            update_last_error(e);
            return ptr::null_mut();
        }
    };

    let db_type = db_schema.borrow_db_type();
    let mutate = match fmt_first_stage_mutate(db_type, db_schema.borrow_inner(), &api_request, &env) {
        Ok((sql, params)) => {
            sbz_Statement::new(
                &sql, 
                params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
            )
        }
        Err(e) => {
            update_last_error(e);
            return ptr::null_mut()
        }
    };
    let select = match fmt_second_stage_select(db_type, db_schema.borrow_inner(), &api_request, &env) {
        Ok((sql, params)) => {
            sbz_Statement::new(
                &sql, 
                params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()
            )
        }
        Err(e) => {
            update_last_error(e);
            return ptr::null_mut()
        }
    };

    let two_stage_statement = sbz_TwoStageStatement::new(db_type.clone(), mutate, select);
    //println!("Rust two_stage_statement: {:?}", two_stage_statement);
    Box::into_raw(Box::new(two_stage_statement))
}

/// Get the mutate statement from a `sbz_TwoStageStatement`.
/// # Safety
/// 
/// # Parameters
/// - `two_stage_statement` - A pointer to the `sbz_TwoStageStatement`.
/// 
/// # Returns
/// A pointer to the `sbz_Statement` representing the mutate statement.
#[no_mangle]
pub unsafe extern "C" fn sbz_two_stage_statement_mutate(two_stage_statement: *const sbz_TwoStageStatement) -> *const sbz_Statement {
    check_null_ptr!(two_stage_statement, "Null pointer passed into two_stage_statement_mutate()");
    let two_stage_statement = unsafe { &*two_stage_statement };
    &two_stage_statement.mutate
}

/// Get the select statement from a `sbz_TwoStageStatement`.
/// # Safety
/// 
/// # Parameters
/// - `two_stage_statement` - A pointer to the `sbz_TwoStageStatement`.
/// 
/// # Returns
/// A pointer to the `sbz_Statement` representing the select statement.
/// 
/// # Note
/// The ids of the mutated rows need to be set before calling this function using `sbz_two_stage_statement_set_ids`.
/// 
#[no_mangle]
pub unsafe extern "C" fn sbz_two_stage_statement_select(two_stage_statement: *const sbz_TwoStageStatement) -> *const sbz_Statement {
    check_null_ptr!(two_stage_statement, "Null pointer passed into two_stage_statement_select()");
    let two_stage_statement = unsafe { &*two_stage_statement };
    if !two_stage_statement.ids_set {
        update_last_error(CoreError::InternalError {
            message: "Ids not set".to_string(),
        });
        return ptr::null();
    }
    &two_stage_statement.select
}

/// Set the ids for a `sbz_TwoStageStatement`.
/// # Safety
/// 
/// # Parameters
/// - `two_stage_statement` - A pointer to the `sbz_TwoStageStatement`.
/// - `ids` - An array of strings representing the ids of the mutated rows.
/// - `ids_count` - The number of ids in the `ids` array.
/// 
/// # Returns
/// 0 if successful, -1 if an error occurred.
/// 
/// # Example
/// ```c
/// const char *ids[] = {"1", "2", "3"};
/// const int ids_set = sbz_two_stage_statement_set_ids(stmt, ids, 3);
/// ```
/// 
#[no_mangle]
pub unsafe extern "C" fn sbz_two_stage_statement_set_ids(two_stage_statement: *mut sbz_TwoStageStatement, ids: *const *const c_char, ids_count: c_int)
-> c_int {
    check_null_ptr!(two_stage_statement, -1, "Null pointer passed into two_stage_statement_set_ids()");
    let two_stage_statement = unsafe { &mut *two_stage_statement };
    let ids = slice::from_raw_parts(ids, ids_count as usize);
    two_stage_statement.set_ids(ids.iter().map(|id| CStr::from_ptr(*id).to_str().unwrap_or_default()).collect());
    0
}

/// Free the memory associated with a `sbz_TwoStageStatement`.
/// # Safety
/// 
/// # Parameters
/// - `two_stage_statement` - A pointer to the `sbz_TwoStageStatement` to free.
///
#[no_mangle]
pub unsafe extern "C" fn sbz_two_stage_statement_free(two_stage_statement: *mut sbz_TwoStageStatement) {
    if !two_stage_statement.is_null() {
        unsafe {
            drop(Box::from_raw(two_stage_statement));
        }
    }
}


/// Create a new `sbz_Statement`
/// # Safety
/// 
/// # Parameters
/// - `schema_name` - The name of the database schema for the current request (ex: public).
/// - `path_prefix` - The prefix of the path for the current request (ex: /api/).
/// - `db_schema` - A pointer to the `sbz_DbSchema`
/// - `request` - A pointer to the `sbz_HTTPRequest`
/// - `max_rows` - The maximum number of rows to return (pass NULL if there is no limit, otherwise pass a string representing the number of rows).
/// 
/// # Returns
/// A pointer to the newly created `sbz_Statement` or a null pointer if an error occurred.
/// 
/// # Example
/// ```c
/// const char* db_type = "sqlite";
/// sbz_DbSchema* db_schema = sbz_db_schema_new(db_type, db_schema_json); // see db_schema_new example for db_schema_json
/// Tuple headers[] = {{"Content-Type", "application/json"}, {"Accept", "application/json"}};
/// Tuple env[] = {{"user_id", "1"}};
/// sbz_HTTPRequest req = {
///   "GET",
///   "http://localhost/rest/projects?select=id,name",
///   headers, 2,
///   NULL,
///   env, 1
/// };
/// sbz_Statement* stmt = sbz_statement_new(
///   "public",
///   "/rest/",
///   db_schema,
///   &req,
///   NULL
/// );
///
/// if (stmt == NULL) {
///   const int err_len = sbz_last_error_length();
///   char* err = (char*)malloc(err_len);
///   sbz_last_error_message(err, err_len);
///   printf("Error: %s\n", err);
///   free(err);
///   return;
/// }
/// 
/// const char* sql = sbz_statement_sql(stmt);
/// const char *const * params = sbz_statement_params(stmt);
/// const char *const * params_types = sbz_statement_params_types(stmt);
/// int params_count = sbz_statement_params_count(stmt);
/// printf("SQL: %s\n", sql);
/// printf("params: %s\n", params[0]);
/// printf("params_count: %d\n", params_count);
/// printf("params_types: %s\n", params_types[0]);
/// ```
#[no_mangle]
pub unsafe extern "C" fn sbz_statement_new(
    schema_name: *const c_char,
    path_prefix: *const c_char,
    db_schema: *const sbz_DbSchema,
    request: *const sbz_HTTPRequest,
    max_rows: *const c_char,
) -> *mut sbz_Statement {
    check_null_ptr!(db_schema, "Null pointer passed as the schema");
    let db_schema = unsafe { &*db_schema };
    check_null_ptr!(request, "Null pointer passed as the request");
    let request = unsafe { &*request };
    let schema_name_str = try_cstr_to_str!(schema_name, "Invalid UTF-8 in schema_name");
    let path_prefix_str = try_cstr_to_str!(path_prefix, "Invalid UTF-8 in path_prefix");
    let method_str = try_cstr_to_str!(request.method, "Invalid UTF-8 in method");
    let body_str = if request.body.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(request.body, "Invalid UTF-8 in body"))
    };
    let uri_str = try_cstr_to_str!(request.uri, "Invalid UTF-8 in uri");
    let parsed_uri = match Url::parse(uri_str) {
        Ok(u) => u,
        Err(e) => {
            update_last_error(CoreError::InternalError {
                message: format!("Unable to parse uri: {}", e),
            });
            return ptr::null_mut();
        }
    };
    let query_pairs: Vec<(String, String)> = parsed_uri
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    let get: Vec<(&str, &str)> = query_pairs.iter().map(|(key, value)| (key.as_ref(), value.as_ref())).collect();

    let headers = match tuples_to_vec(request.headers, request.headers_count as usize) {
        Ok(v) => HashMap::from_iter(v),
        Err(e) => {
            update_last_error(CoreError::InternalError { message: e.to_string() });
            return ptr::null_mut();
        }
    };
    let env = match tuples_to_vec(request.env, request.env_count as usize) {
        Ok(v) => HashMap::from_iter(v),
        Err(e) => {
            update_last_error(CoreError::InternalError { message: e.to_string() });
            return ptr::null_mut();
        }
    };
    let cookies = extract_cookies(headers.get("Cookie").copied());
    let max_rows_opt = if max_rows.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(max_rows, "Invalid UTF-8 in max_rows"))
    };

    let object_str = match parsed_uri.path().strip_prefix(path_prefix_str) {
        Some(s) => s,
        None => {
            update_last_error(CoreError::InternalError {
                message: format!("Unable to strip prefix: {}", path_prefix_str),
            });
            return ptr::null_mut();
        }
    };
    let api_request_result = parse(
        schema_name_str,
        object_str,
        db_schema.borrow_inner(),
        method_str,
        parsed_uri.path(),
        get,
        body_str,
        headers,
        cookies,
        max_rows_opt,
    );

    let api_request = match api_request_result {
        Ok(r) => r,
        Err(e) => {
            update_last_error(e);
            return ptr::null_mut();
        }
    };

    let ApiRequest {
        method,
        schema_name,
        query,
        preferences,
        accept_content_type,
        ..
    } = api_request;

    let db_type = db_schema.borrow_db_type();
    let statement = match db_type.as_str() {
        #[cfg(feature = "postgresql")]
        "postgresql" => {
            postgresql::fmt_main_query_internal(db_schema.borrow_inner(), schema_name, method, &accept_content_type, &query, &preferences, &env)
                .map(postgresql::generate)
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            clickhouse::fmt_main_query_internal(db_schema.borrow_inner(), schema_name, method, &accept_content_type, &query, &preferences, &env)
                .map(clickhouse::generate)
        }
        #[cfg(feature = "sqlite")]
        "sqlite" => sqlite::fmt_main_query_internal(db_schema.borrow_inner(), schema_name, method, &accept_content_type, &query, &preferences, &env)
            .map(sqlite::generate),
        #[cfg(feature = "mysql")]
        "mysql" => mysql::fmt_main_query_internal(db_schema.borrow_inner(), schema_name, method, &accept_content_type, &query, &preferences, &env)
            .map(mysql::generate),
        _ => Err(CoreError::InternalError {
            message: format!("Unsupported db_type: {}", db_type),
        }),
    };

    match statement {
        Ok((sql, params, _)) => {
            let statement = sbz_Statement::new(
                &sql,
                parameters_to_tuples(db_type, params)
                    .iter()
                    .map(|(k, v)| (k.as_ref(), v.as_ref()))
                    .collect(),
            );
            Box::into_raw(Box::new(statement))
        }
        Err(e) => {
            update_last_error(e);
            ptr::null_mut()
        }
    }
}

/// Get the SQL query from a `sbz_Statement`.
/// # Safety
/// 
/// # Parameters
/// - `statement` - A pointer to the `sbz_Statement`.
/// 
/// # Returns
/// A pointer to the SQL query as a C string.
///
#[no_mangle]
pub unsafe extern "C" fn sbz_statement_sql(statement: *const sbz_Statement) -> *const c_char {
    check_null_ptr!(statement, "Null pointer passed into statement_sql()");
    let statement = unsafe { &*statement };
    statement.sql.as_ptr()
}

/// Get the parameter values from a `sbz_Statement`
/// # Safety
/// 
/// # Parameters
/// - `statement` - A pointer to the `sbz_Statement`.
/// 
/// # Returns
/// A pointer to the parameter values as an array of C strings.
/// 
#[no_mangle]
pub unsafe extern "C" fn sbz_statement_params(statement: *const sbz_Statement) -> *const *const c_char {
    check_null_ptr!(statement, "Null pointer passed into statement_params()");
    let statement = unsafe { &*statement };
    statement.params_values.as_ptr()
}

/// Get the parameter types from a `sbz_Statement`
/// # Safety
/// 
/// # Parameters
/// - `statement` - A pointer to the `sbz_Statement`.
/// 
/// # Returns
/// A pointer to the parameter types as an array of C strings.
/// 
/// # Note
/// The parameter types are the database types of the parameters (ex: text, integer, integer[], etc).
#[no_mangle]
pub unsafe extern "C" fn sbz_statement_params_types(statement: *const sbz_Statement) -> *const *const c_char {
    check_null_ptr!(statement, "Null pointer passed into statement_params_types()");
    let statement = unsafe { &*statement };
    statement.params_types.as_ptr()
}

/// Get the number of parameters from a `sbz_Statement`
/// # Safety
/// 
/// # Parameters
/// - `statement` - A pointer to the `sbz_Statement`.
/// 
/// # Returns
/// The number of parameters as an integer.
/// 
#[no_mangle]
pub unsafe extern "C" fn sbz_statement_params_count(statement: *const sbz_Statement) -> c_int {
    if statement.is_null() {
        update_last_error(CoreError::InternalError {
            message: "Null pointer passed into statement_params_count()".to_string(),
        });
        return -1;
    }
    let statement = unsafe { &*statement };
    statement.params_values.len() as c_int
}

/// Free the memory associated with a `sbz_Statement`.
/// # Safety
/// 
/// # Parameters
/// - `statement` - A pointer to the `sbz_Statement` to free.
/// 

#[no_mangle]
pub unsafe extern "C" fn sbz_statement_free(statement: *mut sbz_Statement) {
    if !statement.is_null() {
        unsafe {
            drop(Box::from_raw(statement));
        }
    }
}

#[no_mangle]
/// Free the memory associated with a `sbz_DbSchema`.
/// # Safety
/// 
/// # Parameters
/// - `schema` - A pointer to the `sbz_DbSchema` to free.
pub unsafe extern "C" fn sbz_db_schema_free(schema: *mut sbz_DbSchema) {
    if !schema.is_null() {
        unsafe {
            drop(Box::from_raw(schema));
        }
    }
}

/// Create a new `sbz_DbSchema` from a JSON string.
/// # Safety
/// This function is marked unsafe because it dereferences raw pointers however
/// we are careful to check for null pointers before dereferencing them.
/// # Parameters
/// - `db_type` - The type of database this schema is for.
///   Currently supported types are "postgresql", "clickhouse", "sqlite", and "mysql".
/// - `db_schema_json` - The JSON string representing the schema.
/// # Returns
/// A pointer to the newly created `DbSchema` or a null pointer if an error occurred.
/// 
/// # Note
/// Constructing the JSON schema is tedious and for this reason we provide "introspection queries" for each database type
/// that can be used to generate the schema JSON.
/// 
/// # Example
/// ```c
/// const char* db_type = "sqlite";
/// const char* db_schema_json = ""
/// "{"
/// "    \"schemas\": ["
/// "        {"
/// "            \"name\": \"public\","
/// "            \"objects\": ["
/// "                {"
/// "                    \"kind\": \"table\","
/// "                    \"name\": \"clients\","
/// "                    \"columns\": ["
/// "                        {"
/// "                            \"name\": \"id\","
/// "                            \"data_type\": \"INTEGER\","
/// "                            \"primary_key\": true"
/// "                        },"
/// "                        {"
/// "                            \"name\": \"name\","
/// "                            \"data_type\": \"TEXT\","
/// "                            \"primary_key\": false"
/// "                        }"
/// "                    ],"
/// "                    \"foreign_keys\": []"
/// "                }"
/// "            ]"
/// "        }"
/// "    ]"
/// "}"
/// ;
/// sbz_DbSchema* db_schema = sbz_db_schema_new(db_type, db_schema_json);
/// if (db_schema == NULL) {
///   const int err_len = sbz_last_error_length();
///   char* err = (char*)malloc(err_len);
///   sbz_last_error_message(err, err_len);
///   printf("Error: %s\n", err);
///   free(err);
///   return;
/// }
/// ```
#[no_mangle]
pub unsafe extern "C" fn sbz_db_schema_new(db_type: *const c_char, db_schema_json: *const c_char) -> *mut sbz_DbSchema {
    // Check for null pointers
    if db_type.is_null() || db_schema_json.is_null() {
        let err = CoreError::InternalError {
            message: "Null pointer passed into db_schema_new()".to_string(),
        };
        update_last_error(err);
        return ptr::null_mut();
    }

    // Convert the C strings to Rust &strs
    let db_type_str = try_cstr_to_str!(db_type, "Invalid UTF-8 in db_type");
    // check if db_type is supported
    if !["postgresql", "clickhouse", "sqlite", "mysql"].contains(&db_type_str) {
        update_last_error(CoreError::InternalError {
            message: format!("Unsupported db_type: {}", db_type_str),
        });
        return ptr::null_mut();
    }

    let mut db_schema_json = try_cstr_to_str!(db_schema_json, "Invalid UTF-8 in db_schema_json").to_owned();
    //println!("db_schema_json: {}", db_schema_json);
    if db_type_str == "clickhouse" {
        //println!("json schema original:\n{:?}\n", s);
        // clickhouse query returns check_json_str and using_json_str as string
        // so we first parse it into a JsonValue and then convert those two fields into json
        let mut v: JsonValue = match serde_json::from_str(&db_schema_json) {
            //.expect("invalid schema json");
            Ok(v) => v,
            Err(e) => {
                update_last_error(CoreError::InternalError {
                    message: format!("Unable to parse json: {}", e),
                });
                return ptr::null_mut();
            }
        };
        //recursively iterate through the json and convert check_json_str and using_json_str into json
        // println!("json value before replace:\n{:?}\n", v);
        // recursively iterate through the json and apply the f function
        if replace_json_str(&mut v).is_err() {
            update_last_error(CoreError::InternalError {
                message: "invalid schema json".to_string(),
            });
            return ptr::null_mut();
        }

        db_schema_json = match serde_json::to_string_pretty(&v) {
            //.expect("invalid schema json");
            Ok(s) => s,
            Err(e) => {
                update_last_error(CoreError::InternalError {
                    message: format!("Unable to convert json to string: {}", e),
                });
                return ptr::null_mut();
            }
        };
    }

    let db_schema = sbz_DbSchema::try_new(db_type_str.to_string(), db_schema_json, |data_ref| {
        let s = data_ref.as_str();
        serde_json::from_str(s)
    });

    match db_schema {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(e) => {
            update_last_error(e);
            ptr::null_mut()
        }
    }
}

/// Write the most recent error message into a caller-provided buffer as a UTF-8
/// string, returning the number of bytes written.
///
/// # Safety
/// This function is marked unsafe because it dereferences raw pointers however
/// we are careful to check for null pointers before dereferencing them.
///
/// # Note
///
/// This writes a **UTF-8** string into the buffer. Windows users may need to
/// convert it to a UTF-16 "unicode" afterwards.
///
/// # Parameters
/// - `buffer` - A pointer to a buffer to write the error message into.
/// - `length` - The length of the buffer.
///
/// # Returns
/// If there are no recent errors then this returns `0` (because we wrote 0
/// bytes). `-1` is returned if there are any errors, for example when passed a
/// null pointer or a buffer of insufficient size.
#[no_mangle]
pub unsafe extern "C" fn sbz_last_error_message(buffer: *mut c_char, length: c_int) -> c_int {
    if buffer.is_null() {
        //warn!("Null pointer passed into last_error_message() as the buffer");
        return -1;
    }

    let last_error = match take_last_error() {
        Some(err) => err,
        None => return 0,
    };

    let error_message = last_error.to_string();

    let buffer = slice::from_raw_parts_mut(buffer as *mut u8, length as usize);

    if error_message.len() >= buffer.len() {
        return -1;
    }

    ptr::copy_nonoverlapping(error_message.as_ptr(), buffer.as_mut_ptr(), error_message.len());

    // Add a trailing null so people using the string as a `char *` don't
    // accidentally read into garbage.
    buffer[error_message.len()] = 0;

    error_message.len() as c_int
}

/// Update the most recent error, clearing whatever may have been there before.
pub fn update_last_error<E: StdError + 'static>(err: E) {
    {
        // Print a pseudo-backtrace for this error, following back each error's
        // cause until we reach the root error.
        let mut source = err.source();
        while let Some(parent_err) = source {
            source = parent_err.source();
        }
    }

    LAST_ERROR.with(|prev| {
        *prev.borrow_mut() = Some(Box::new(err));
    });
}

/// Retrieve the most recent error, clearing it in the process.
pub fn take_last_error() -> Option<Box<dyn StdError>> {
    LAST_ERROR.with(|prev| prev.borrow_mut().take())
}

/// Calculate the number of bytes in the last error's error message **not**
/// including any trailing `null` characters.
#[no_mangle]
pub extern "C" fn sbz_last_error_length() -> c_int {
    LAST_ERROR.with(|prev| match *prev.borrow() {
        Some(ref err) => err.to_string().len() as c_int + 1,
        None => 0,
    })
}
