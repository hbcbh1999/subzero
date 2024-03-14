#![allow(non_camel_case_types)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::ffi::{CStr, CString};
use std::ptr;
use std::fs;
use libc::{c_char, c_int};
// use std::collections::HashMap;
use serde_json::Value as JsonValue;
use std::cell::RefCell;
//use std::error::Error as StdError;
use std::slice;
use ouroboros::self_referencing;
use std::collections::HashMap;
use url::Url;
use subzero_core::{
    parser::postgrest::parse,
    schema::{DbSchema as CoreDbSchema, replace_json_str},
    api::{ApiRequest, DEFAULT_SAFE_SELECT_FUNCTIONS},
    permissions::{check_privileges, check_safe_functions, insert_policy_conditions, replace_select_star},
    error::Error as CoreError,
};
use crate::{check_null_ptr, try_cstr_to_str, try_cstr_to_cstring, cstr_to_str_unchecked, unwrap_result_or_return};
use crate::utils::{
    extract_cookies, arr_to_tuple_vec, parameters_to_tuples, fmt_first_stage_mutate, fmt_second_stage_select, fmt_mysql_env_query,
    fmt_postgresql_env_query, fmt_introspection_query,
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
    static LAST_ERROR: RefCell<Option<Box<CoreError>>> = RefCell::new(None);
    static LAST_ERROR_LENGTH: RefCell<c_int> = RefCell::new(0);
    static LAST_ERROR_HTTP_STATUS: RefCell<c_int> = RefCell::new(0);

}
//static NULL_PTR: *const c_char = std::ptr::null();
static DISABLED: AtomicBool = AtomicBool::new(false);

/// A structure for holding the information about database entities and permissions (tables, views, etc).
#[self_referencing]
pub struct sbz_DbSchema {
    db_type: String,
    license_key: Option<String>,
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
    params_types: Vec<*const c_char>,
    params_values: Vec<*const c_char>,
}
impl sbz_Statement {
    pub fn new(sql: &str, params: Vec<(&str, &str)>) -> Self {
        let sql = CString::new(sql).unwrap();
        let _params: Vec<(CString, CString)> = params
            .into_iter()
            .map(|(key, value)| (CString::new(key).unwrap(), CString::new(value).unwrap()))
            .collect();

        let params_types: Vec<*const c_char> = _params.iter().map(|(_, type_)| type_.as_ptr()).collect();
        let params_values: Vec<*const c_char> = _params.iter().map(|(value, _)| value.as_ptr()).collect();
        sbz_Statement {
            sql,
            _params,
            params_types,
            params_values,
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
        sbz_TwoStageStatement {
            db_type,
            mutate,
            select,
            ids_set: false,
        }
    }
    pub fn set_ids(&mut self, ids: Vec<&str>) {
        let select_params = &self.select._params;
        // find the param equal to _subzero_ids_placeholder_ and replace it with the ids
        let placehoder_vec = vec!["_subzero_ids_placeholder_"];
        let placehoder = CString::new(match self.db_type.as_str() {
            "sqlite" | "mysql" => serde_json::to_string(&placehoder_vec).unwrap_or_default(),
            _ => format!("'{{{}}}'", placehoder_vec.join(", ")),
        })
        .unwrap();
        let mut pos = None;
        for (i, (val, _)) in select_params.iter().enumerate() {
            if val == &placehoder {
                pos = Some(i);
            }
        }
        let new_val = CString::new(match self.db_type.as_str() {
            "sqlite" | "mysql" => serde_json::to_string(&ids).unwrap_or_default(),
            _ => format!("'{{{}}}'", ids.join(", ")),
        })
        .unwrap();
        if let Some(pos) = pos {
            self.select._params[pos].0 = new_val;
            self.select.params_values[pos] = self.select._params[pos].0.as_ptr();
            self.ids_set = true;
        }
    }
}

/// A structure representing a HTTP request.
/// This is used to pass the information about the request to the subzero core.

pub struct sbz_HTTPRequest {
    method: *const c_char,
    #[allow(dead_code)]
    method_owned: Option<CString>,
    uri: *const c_char,
    #[allow(dead_code)]
    uri_owned: Option<CString>,
    body: Option<*const c_char>,
    #[allow(dead_code)]
    body_owned: Option<CString>,
    headers: Vec<(*const c_char, *const c_char)>,
    #[allow(dead_code)]
    headers_owned: Option<Vec<(CString, CString)>>,
    env: Vec<(*const c_char, *const c_char)>,
    #[allow(dead_code)]
    env_owned: Option<Vec<(CString, CString)>>,
}

/// Create a new `sbz_HTTPRequest` and take ownership of the strings.
/// This is usefull when the caller can not guarantee that the strings will be valid for the lifetime of the `sbz_HTTPRequest`.
/// # Safety
///
/// # Parameters
/// - `method` - The HTTP method (GET, POST, PUT, DELETE, etc).
/// - `uri` - The full URI of the request (including the query string, ex: http://example.com/path?query=string).
/// - `body` - The body of the request (pass NULL if there is no body).
/// - `headers` - An array of key-value pairs representing the headers of the request, it needs to contain an even number of elements.
/// - `headers_count` - The number of elements in the `headers` array.
/// - `env` - An array of key-value pairs representing the environment data that needs to be available to the query. It needs to contain an even number of elements.
/// - `env_count` - The number of elements in the `env` array.
///
/// # Returns
/// A pointer to the newly created `sbz_HTTPRequest` or a null pointer if an error occurred.
///
/// # Example
/// ```c
/// const char* headers[] = {"Content-Type", "application/json", "Accept", "application/json"};
/// const char* env[] = {"user_id", "1"};
/// sbz_HTTPRequest* req = sbz_http_request_new(
///    "POST",
///    "http://localhost/rest/projects?select=id,name",
///    "[{\"name\":\"project1\"}]",
///    headers, 4,
///    env, 2
/// );
/// ```
///
#[no_mangle]
pub unsafe extern "C" fn sbz_http_request_new_with_clone(
    method: *const c_char, uri: *const c_char, body: *const c_char, headers: *const *const c_char, headers_count: c_int, env: *const *const c_char,
    env_count: c_int,
) -> *mut sbz_HTTPRequest {
    let method_str = try_cstr_to_cstring!(method, "Invalid UTF-8 or null pointer in method");
    let uri_str = try_cstr_to_cstring!(uri, "Invalid UTF-8 or null pointer in uri");
    let body_str = if body.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(body, "Invalid UTF-8 or null pointer in body"))
    };
    let headers = match arr_to_tuple_vec(headers, headers_count as usize) {
        Ok(v) => v,
        Err(e) => {
            update_last_error(CoreError::InternalError { message: e.to_string() });
            return ptr::null_mut();
        }
    };
    let env = match arr_to_tuple_vec(env, env_count as usize) {
        Ok(v) => v,
        Err(e) => {
            update_last_error(CoreError::InternalError { message: e.to_string() });
            return ptr::null_mut();
        }
    };
    let method_owned = Some(method_str);
    let uri_owned = Some(uri_str);
    let body_owned = body_str.map(|s| CString::new(s).unwrap());
    let headers_owned: Option<Vec<(CString, CString)>> = Some(
        headers
            .iter()
            .map(|(k, v)| unsafe { (CStr::from_ptr(*k).to_owned(), CStr::from_ptr(*v).to_owned()) })
            .collect(),
    );
    let env_owned: Option<Vec<(CString, CString)>> = Some(
        env.iter()
            .map(|(k, v)| unsafe { (CStr::from_ptr(*k).to_owned(), CStr::from_ptr(*v).to_owned()) })
            .collect(),
    );
    let request = sbz_HTTPRequest {
        method: method_owned.as_ref().map(|s| s.as_ptr()).unwrap(),
        method_owned,
        uri: uri_owned.as_ref().map(|s| s.as_ptr()).unwrap(),
        uri_owned,
        body: body_owned.as_ref().map(|s| s.as_ptr()),
        body_owned,
        headers: match &headers_owned {
            Some(v) => v.iter().map(|(k, v)| (k.as_ptr(), v.as_ptr())).collect(),
            None => vec![],
        },
        headers_owned,
        env: match &env_owned {
            Some(v) => v.iter().map(|(k, v)| (k.as_ptr(), v.as_ptr())).collect(),
            None => vec![],
        },
        env_owned,
    };
    Box::into_raw(Box::new(request))
}

/// Create a new `sbz_HTTPRequest`
/// # Safety
///
/// # Parameters
/// - `method` - The HTTP method (GET, POST, PUT, DELETE, etc).
/// - `uri` - The full URI of the request (including the query string, ex: http://example.com/path?query=string).
/// - `body` - The body of the request (pass NULL if there is no body).
/// - `headers` - An array of key-value pairs representing the headers of the request, it needs to contain an even number of elements.
/// - `headers_count` - The number of elements in the `headers` array.
/// - `env` - An array of key-value pairs representing the environment data that needs to be available to the query. It needs to contain an even number of elements.
/// - `env_count` - The number of elements in the `env` array.
///
/// # Returns
/// A pointer to the newly created `sbz_HTTPRequest` or a null pointer if an error occurred.
///
/// # Example
/// ```c
/// const char* headers[] = {"Content-Type", "application/json", "Accept", "application/json"};
/// const char* env[] = {"user_id", "1"};
/// sbz_HTTPRequest* req = sbz_http_request_new(
///   "POST",
///   "http://localhost/rest/projects?select=id,name",
///   "[{\"name\":\"project1\"}]",
///   headers, 4,
///   env, 2
/// );
/// ```
///
#[no_mangle]
pub unsafe extern "C" fn sbz_http_request_new(
    method: *const c_char, uri: *const c_char, body: *const c_char, headers: *const *const c_char, headers_count: c_int, env: *const *const c_char,
    env_count: c_int,
) -> *mut sbz_HTTPRequest {
    try_cstr_to_str!(method, "Invalid UTF-8 or null pointer in method");
    try_cstr_to_str!(uri, "Invalid UTF-8 or null pointer in uri");
    let body_str = if body.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(body, "Invalid UTF-8 or null pointer in body"))
    };
    let headers = match arr_to_tuple_vec(headers, headers_count as usize) {
        Ok(v) => v,
        Err(e) => {
            update_last_error(CoreError::InternalError { message: e.to_string() });
            return ptr::null_mut();
        }
    };
    let env = match arr_to_tuple_vec(env, env_count as usize) {
        Ok(v) => v,
        Err(e) => {
            update_last_error(CoreError::InternalError { message: e.to_string() });
            return ptr::null_mut();
        }
    };

    let request = sbz_HTTPRequest {
        method,
        method_owned: None,
        uri,
        uri_owned: None,
        body: if body_str.is_some() { Some(body) } else { None },
        body_owned: None,
        headers,
        headers_owned: None,
        env,
        env_owned: None,
    };
    Box::into_raw(Box::new(request))
}

/// Free the memory associated with a `sbz_HTTPRequest`.
/// # Safety
///
/// # Parameters
/// - `request` - A pointer to the `sbz_HTTPRequest` to free.
///
#[no_mangle]
pub unsafe extern "C" fn sbz_http_request_free(request: *mut sbz_HTTPRequest) {
    if !request.is_null() {
        unsafe {
            drop(Box::from_raw(request));
        }
    }
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
/// sbz_DbSchema* db_schema = sbz_db_schema_new(db_type, db_schema_json, NULL); // see db_schema_new example for db_schema_json
/// const char* headers[] = {"Content-Type", "application/json", "Accept", "application/json"};
/// const char* env[] = {"user_id", "1"};
/// sbz_HTTPRequest* req = sbz_http_request_new(
///    "POST",
///    "http://localhost/rest/projects?select=id,name",
///    "[{\"name\":\"project1\"}",
///    headers, 4,
///    env, 2
/// );
/// sbz_TwoStageStatement* stmt = sbz_two_stage_statement_new(
///     "public",
///     "/rest/",
///     db_schema,
///     req,
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
/// sbz_http_request_free(req);
/// sbz_db_schema_free(db_schema);
/// ```
#[no_mangle]
pub unsafe extern "C" fn sbz_two_stage_statement_new(
    schema_name: *const c_char, path_prefix: *const c_char, role: *const c_char, db_schema: *const sbz_DbSchema, request: *const sbz_HTTPRequest,
    max_rows: *const c_char,
) -> *mut sbz_TwoStageStatement {
    // check if not disabled
    if DISABLED.load(Ordering::Relaxed) {
        update_last_error(CoreError::InternalError {
            message: "Subzero is disabled".to_string(),
        });
        return ptr::null_mut();
    }
    check_null_ptr!(db_schema, "Null pointer passed as the schema");
    let db_schema = unsafe { &*db_schema };
    check_null_ptr!(request, "Null pointer passed as the request");
    let request = unsafe { &*request };
    let schema_name_str = try_cstr_to_str!(schema_name, "Invalid UTF-8 or null pointer in schema_name");
    let path_prefix_str = try_cstr_to_str!(path_prefix, "Invalid UTF-8 or null pointer in path_prefix");
    let role_str = if role.is_null() {
        "" // permission checking functions need a string
    } else {
        try_cstr_to_str!(role, "Invalid UTF-8 or null pointer in role")
    };
    let method_str = cstr_to_str_unchecked!(request.method);
    let body_str = request.body.map(|b| cstr_to_str_unchecked!(b));
    let uri_str = cstr_to_str_unchecked!(request.uri);
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

    let headers: HashMap<_, _> = request
        .headers
        .iter()
        .map(|(k, v)| (cstr_to_str_unchecked!(*k), cstr_to_str_unchecked!(*v)))
        .collect();
    let env: HashMap<_, _> = request
        .env
        .iter()
        .map(|(k, v)| (cstr_to_str_unchecked!(*k), cstr_to_str_unchecked!(*v)))
        .collect();
    let cookies = extract_cookies(headers.get("Cookie").copied());
    let max_rows_opt = if max_rows.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(max_rows, "Invalid UTF-8 or null pointer in max_rows"))
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

    let mut api_request = unwrap_result_or_return!(api_request_result);

    // replace "*" with the list of columns the user has access to
    // so that he does not encounter permission errors
    unwrap_result_or_return!(replace_select_star(db_schema.borrow_inner(), schema_name_str, role_str, &mut api_request.query));
    unwrap_result_or_return!(run_privileges_checks(db_schema.borrow_inner(), schema_name_str, role_str, &api_request));
    unwrap_result_or_return!(insert_policy_conditions(db_schema.borrow_inner(), schema_name_str, role_str, &mut api_request.query));

    let db_type = db_schema.borrow_db_type();
    let mutate = match fmt_first_stage_mutate(db_type, db_schema.borrow_inner(), &api_request, &env) {
        Ok((sql, params)) => sbz_Statement::new(&sql, params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()),
        Err(e) => {
            update_last_error(e);
            return ptr::null_mut();
        }
    };
    let select = match fmt_second_stage_select(db_type, db_schema.borrow_inner(), &api_request, &env) {
        Ok((sql, params)) => sbz_Statement::new(&sql, params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()),
        Err(e) => {
            update_last_error(e);
            return ptr::null_mut();
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
pub unsafe extern "C" fn sbz_two_stage_statement_set_ids(
    two_stage_statement: *mut sbz_TwoStageStatement, ids: *const *const c_char, ids_count: c_int,
) -> c_int {
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

/// Create a new `sbz_Statement` for the main query.
/// # Safety
///
/// # Parameters
/// - `schema_name` - The name of the database schema for the current request (ex: public).
/// - `path_prefix` - The prefix of the path for the current request (ex: /api/).
/// - `role` - The role of the user making the request
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
/// sbz_DbSchema* db_schema = sbz_db_schema_new(db_type, db_schema_json, NULL); // see db_schema_new example for db_schema_json
/// sbz_Tuple headers[] = {{"Content-Type", "application/json"}, {"Accept", "application/json"}};
/// sbz_Tuple env[] = {{"user_id", "1"}};
/// sbz_HTTPRequest* req = sbz_http_request_new(
///    "GET",
///    "http://localhost/rest/projects?select=id,name",
///    NULL,
///    headers, 4,
///    env, 2
/// );
/// sbz_Statement* stmt = sbz_statement_new(
///   "public",
///   "/rest/",
///   "user",
///   db_schema,
///   req,
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
/// sbz_statement_free(stmt);
/// sbz_http_request_free(req);
/// sbz_db_schema_free(db_schema);
/// ```
#[no_mangle]
pub unsafe extern "C" fn sbz_statement_main_new(
    schema_name: *const c_char, path_prefix: *const c_char, role: *const c_char, db_schema: *const sbz_DbSchema, request: *const sbz_HTTPRequest,
    max_rows: *const c_char,
) -> *mut sbz_Statement {
    if DISABLED.load(Ordering::Relaxed) {
        update_last_error(CoreError::InternalError {
            message: "Subzero is disabled".to_string(),
        });
        return ptr::null_mut();
    }
    check_null_ptr!(db_schema, "Null pointer passed as the schema");
    let db_schema = unsafe { &*db_schema };
    check_null_ptr!(request, "Null pointer passed as the request");
    let request = unsafe { &*request };
    let schema_name_str = try_cstr_to_str!(schema_name, "Invalid UTF-8 or null pointer in schema_name");
    let path_prefix_str = try_cstr_to_str!(path_prefix, "Invalid UTF-8 or null pointer in path_prefix");
    let role_str = if role.is_null() {
        "" // permission checking functions need a string
    } else {
        try_cstr_to_str!(role, "Invalid UTF-8 or null pointer in role")
    };
    let method_str = cstr_to_str_unchecked!(request.method);
    let body_str = request.body.map(|b| cstr_to_str_unchecked!(b));
    let uri_str = cstr_to_str_unchecked!(request.uri);

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

    let headers: HashMap<_, _> = request
        .headers
        .iter()
        .map(|(k, v)| (cstr_to_str_unchecked!(*k), cstr_to_str_unchecked!(*v)))
        .collect();
    let env: HashMap<_, _> = request
        .env
        .iter()
        .map(|(k, v)| (cstr_to_str_unchecked!(*k), cstr_to_str_unchecked!(*v)))
        .collect();
    let cookies = extract_cookies(headers.get("Cookie").copied());
    let max_rows_opt = if max_rows.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(max_rows, "Invalid UTF-8 or null pointer in max_rows"))
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

    let mut api_request = unwrap_result_or_return!(api_request_result);

    // replace "*" with the list of columns the user has access to
    // so that he does not encounter permission errors
    unwrap_result_or_return!(replace_select_star(db_schema.borrow_inner(), schema_name_str, role_str, &mut api_request.query));
    unwrap_result_or_return!(run_privileges_checks(db_schema.borrow_inner(), schema_name_str, role_str, &api_request));
    unwrap_result_or_return!(insert_policy_conditions(db_schema.borrow_inner(), schema_name_str, role_str, &mut api_request.query));

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

fn run_privileges_checks<'a>(
    db_schema: &'a CoreDbSchema, schema_name: &'a str, role: &'a str, api_request: &'a ApiRequest<'a>,
) -> Result<(), CoreError> {
    // in case when the role is not set (but authenticated through jwt) the query will be executed with the privileges
    // of the "authenticator" role unless the DbSchema has internal privileges set
    let allowed_select_functions = Vec::from(DEFAULT_SAFE_SELECT_FUNCTIONS);
    check_privileges(db_schema, schema_name, role, api_request)?;
    check_safe_functions(api_request, &allowed_select_functions)?;
    Ok(())
}

/// Create a new `sbz_Statement` for the env query
/// # Safety
/// # Parameters
/// - `db_schema` - A pointer to the `sbz_DbSchema`
/// - `request` - A pointer to the `sbz_HTTPRequest`
///
/// # Returns
/// A pointer to the newly created `sbz_Statement` or a null pointer if an error occurred.
#[no_mangle]
pub unsafe extern "C" fn sbz_statement_env_new(db_schema: *const sbz_DbSchema, request: *const sbz_HTTPRequest) -> *mut sbz_Statement {
    if DISABLED.load(Ordering::Relaxed) {
        update_last_error(CoreError::InternalError {
            message: "Subzero is disabled".to_string(),
        });
        return ptr::null_mut();
    }
    check_null_ptr!(db_schema, "Null pointer passed as the schema");
    let db_schema = unsafe { &*db_schema };
    check_null_ptr!(request, "Null pointer passed as the request");
    let request = unsafe { &*request };

    let env: HashMap<_, _> = request
        .env
        .iter()
        .map(|(k, v)| (cstr_to_str_unchecked!(*k), cstr_to_str_unchecked!(*v)))
        .collect();
    let db_type = db_schema.borrow_db_type();
    let statement = match db_type.as_str() {
        #[cfg(feature = "postgresql")]
        "postgresql" => Ok(postgresql::generate(fmt_postgresql_env_query(&env))),
        #[cfg(feature = "mysql")]
        "mysql" => Ok(mysql::generate(fmt_mysql_env_query(&env))),
        _ => Err(CoreError::InternalError {
            message: format!("Unsupported db_type for the env query: {}", db_type),
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
    //statement.params_values.as_ptr()
    if statement.params_values.is_empty() {
        //&NULL_PTR
        std::ptr::null()
    } else {
        statement.params_values.as_ptr()
    }
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
    //statement.params_types.as_ptr()
    if statement.params_types.is_empty() {
        //&NULL_PTR
        std::ptr::null()
    } else {
        statement.params_types.as_ptr()
    }
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
/// - `license_key` - The license key for the subzero core.
///   Pass NULL if you are running in demo mode.
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
/// sbz_DbSchema* db_schema = sbz_db_schema_new(db_type, db_schema_json, NULL);
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
pub unsafe extern "C" fn sbz_db_schema_new(db_type: *const c_char, db_schema_json: *const c_char, license_key: *const c_char) -> *mut sbz_DbSchema {
    // Convert the C strings to Rust &strs
    let db_type_str = try_cstr_to_str!(db_type, "Invalid UTF-8 or null pointer in db_type");
    // check if db_type is supported
    if !["postgresql", "clickhouse", "sqlite", "mysql"].contains(&db_type_str) {
        update_last_error(CoreError::InternalError {
            message: format!("Unsupported db_type: {}", db_type_str),
        });
        return ptr::null_mut();
    }

    let license_key = if license_key.is_null() {
        None
    } else {
        let k = try_cstr_to_str!(license_key, "Invalid UTF-8 in license_key").to_owned();
        // no checks for now except for empty string
        match k.is_empty() {
            true => Some(k),
            false => None,
        }
    };

    let mut db_schema_json = try_cstr_to_str!(db_schema_json, "Invalid UTF-8 or null pointer in db_schema_json").to_owned();
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

    if license_key.is_none() {
        // start a thread and set the DISABLED flag to true after 15 minutes
        let _ = thread::spawn(|| {
            thread::sleep(Duration::from_secs(900));
            DISABLED.store(true, Ordering::Relaxed);
        });
    }
    let db_schema = sbz_DbSchema::try_new(db_type_str.to_string(), license_key, db_schema_json, |data_ref| {
        let s = data_ref.as_str();
        serde_json::from_str(s)
    });

    match db_schema {
        Ok(s) => Box::into_raw(Box::new(s)),
        Err(e) => {
            update_last_error(CoreError::Serde { source: e });
            ptr::null_mut()
        }
    }
}

/// Check if subzero is running in demo mode.
/// # Safety
///
/// # Parameters
/// - `db_schema` - A pointer to the `sbz_DbSchema`.
///
/// # Returns
/// 1 if subzero is running in demo mode, 0 if it is not, -1 if an error occurred.
///
#[no_mangle]
pub unsafe extern "C" fn sbz_db_schema_is_demo(db_schema: *const sbz_DbSchema) -> c_int {
    check_null_ptr!(db_schema, -1, "Null pointer passed into db_schema_is_demo()");
    let db_schema = unsafe { &*db_schema };
    if db_schema.borrow_license_key().is_none() {
        1
    } else {
        0
    }
}

/// Get the introspection query for a database type.
///
/// # Safety
///
/// # Parameters
/// - `db_type` - The type of database to get the introspection query for.
/// - `path` - The path to the directory where the introspection query files are located.
/// - `custom_relations` - An optional JSON string representing custom relations to include in the introspection query.
/// - `custom_permissions` - An optional JSON string representing custom permissions to include in the introspection query.
///
/// # Returns
/// A pointer to the introspection query as a C string.
#[no_mangle]
pub unsafe extern "C" fn sbz_introspection_query(
    db_type: *const c_char, path: *const c_char, custom_relations: *const c_char, custom_permissions: *const c_char,
) -> *mut c_char {
    let db_type = try_cstr_to_str!(db_type, "Invalid UTF-8 or null pointer in db_type");
    let path = try_cstr_to_str!(path, "Invalid UTF-8 or null pointer in path");
    let custom_relations = if custom_relations.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(custom_relations, "Invalid UTF-8 or null pointer in custom_relations"))
    };
    let custom_permissions = if custom_permissions.is_null() {
        None
    } else {
        Some(try_cstr_to_str!(custom_permissions, "Invalid UTF-8 or null pointer in custom_permissions"))
    };

    let file_name = format!("{}/{}_introspection_query.sql", path, db_type);
    let raw_introspection_query: String = match fs::read_to_string(file_name) {
        Ok(s) => s,
        Err(e) => {
            update_last_error(CoreError::InternalError {
                message: format!("Unable to read file: {}", e),
            });
            return ptr::null_mut();
        }
    };
    let introspection_query = fmt_introspection_query(&raw_introspection_query, custom_relations, custom_permissions);
    let cstr = CString::new(introspection_query).unwrap();

    cstr.into_raw()
}

/// Free the memory associated with the introspection query.
///
/// # Safety
///
/// # Parameters
/// - `introspection_query` - A pointer to the introspection query to free.
///
#[no_mangle]
pub unsafe extern "C" fn sbz_introspection_query_free(introspection_query: *mut c_char) {
    if !introspection_query.is_null() {
        unsafe {
            let s = CString::from_raw(introspection_query);
            drop(s);
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

    let error_message = last_error.json_body().to_string();

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
pub fn update_last_error(err: CoreError) {
    // {
    //     // Print a pseudo-backtrace for this error, following back each error's
    //     // cause until we reach the root error.
    //     let mut source = err.source();
    //     while let Some(parent_err) = source {
    //         source = parent_err.source();
    //     }
    //     // print to the console
    //     eprintln!("Rust Error: {}", err);
    // }

    LAST_ERROR_LENGTH.with(|prev| {
        *prev.borrow_mut() = err.json_body().to_string().len() as c_int + 1;
    });
    LAST_ERROR_HTTP_STATUS.with(|prev| {
        *prev.borrow_mut() = err.status_code() as c_int;
    });
    LAST_ERROR.with(|prev| {
        *prev.borrow_mut() = Some(Box::new(err));
    });
}

/// Clear the most recent error.
/// # Safety
///
#[no_mangle]
pub unsafe extern "C" fn sbz_clear_last_error() {
    LAST_ERROR.with(|prev| {
        *prev.borrow_mut() = None;
    });
    LAST_ERROR_LENGTH.with(|prev| {
        *prev.borrow_mut() = 0;
    });
    LAST_ERROR_HTTP_STATUS.with(|prev| {
        *prev.borrow_mut() = 0;
    });
}

/// Retrieve the most recent error, clearing it in the process.
pub fn take_last_error() -> Option<Box<CoreError>> {
    LAST_ERROR.with(|prev| prev.borrow_mut().take())
}

/// Calculate the number of bytes in the last error's error message **not**
/// including any trailing `null` characters.
#[no_mangle]
pub extern "C" fn sbz_last_error_length() -> c_int {
    // LAST_ERROR.with(|prev| match *prev.borrow() {
    //     Some(ref err) => err.to_string().len() as c_int + 1,
    //     None => 0,
    // })
    LAST_ERROR_LENGTH.with(|prev| *prev.borrow())
}

/// Get the HTTP status code of the last error.
///
/// # Returns
/// The HTTP status code of the last error.
///
#[no_mangle]
pub extern "C" fn sbz_last_error_http_status() -> c_int {
    LAST_ERROR_HTTP_STATUS.with(|prev| *prev.borrow())
}
