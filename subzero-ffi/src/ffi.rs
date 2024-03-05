use std::ffi::{CStr, CString};
use std::ptr;
use libc::{c_char, c_int};
// use std::borrow::Cow;
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
    // formatter::{Param::*, ToParam},
    api::ApiRequest,
    // permissions::{check_privileges, check_safe_functions, insert_policy_conditions, replace_select_star},
    error::Error as CoreError,
};
use crate::{check_null_ptr, try_cstr_to_str};
use crate::utils::{extract_cookies, tuples_to_vec, parameters_to_tuples};
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

#[self_referencing]
pub struct DbSchema {
    db_type: String,
    data: String,
    #[covariant]
    #[borrows(data)]
    inner: CoreDbSchema<'this>,
}

///
pub struct Statement {
    sql: CString,
    _params: Vec<(CString, CString)>,
    params_values: Vec<*const c_char>,
    params_types: Vec<*const c_char>,
}
impl Statement {
    pub fn new(sql: &str, params: Vec<(&str, &str)>) -> Self {
        let sql = CString::new(sql).unwrap();
        let _params: Vec<(CString, CString)> = params
            .into_iter()
            .map(|(key, value)| (CString::new(key).unwrap(), CString::new(value).unwrap()))
            .collect();

        let params_values: Vec<*const c_char> = _params.iter().map(|(value, _)| value.as_ptr()).collect();
        let params_types: Vec<*const c_char> = _params.iter().map(|(_, type_)| type_.as_ptr()).collect();

        Statement {
            sql,
            _params,
            params_values,
            params_types,
        }
    }
}

#[repr(C)]
pub struct Tuple {
    pub key: *const c_char,
    pub value: *const c_char,
}

#[repr(C)]
pub struct Request {
    pub method: *const c_char,
    pub uri: *const c_char,
    pub headers: *const Tuple,
    pub headers_count: c_int,
    pub body: *const c_char,
    pub env: *const Tuple,
    pub env_count: c_int,
}

#[no_mangle]
pub extern "C" fn hello_world() -> *const c_char {
    CString::new("Hello, world!").unwrap().into_raw()
}

///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn statement_new(
    schema_name: *const c_char, path_prefix: *const c_char, db_schema: *const DbSchema, request: *const Request, max_rows: *const c_char,
) -> *mut Statement {
    check_null_ptr!(db_schema, "Null pointer passed into fmt_main_statement() as the schema");
    let db_schema = unsafe { &*db_schema };
    check_null_ptr!(request, "Null pointer passed into fmt_main_statement() as the request");
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
            let statement = Statement::new(&sql, parameters_to_tuples(db_type, params));
            Box::into_raw(Box::new(statement))
        }
        Err(e) => {
            update_last_error(e);
            ptr::null_mut()
        }
    }
}

/// Get the SQL query from a `Statement`.
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn statement_sql(statement: *const Statement) -> *const c_char {
    check_null_ptr!(statement, "Null pointer passed into statement_sql()");
    let statement = unsafe { &*statement };
    statement.sql.as_ptr()
}

/// Get the parameter values from a `Statement`
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn statement_params(statement: *const Statement) -> *const *const c_char {
    check_null_ptr!(statement, "Null pointer passed into statement_params()");
    let statement = unsafe { &*statement };
    statement.params_values.as_ptr()
}

/// Get the parameter types from a `Statement`
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn statement_params_types(statement: *const Statement) -> *const *const c_char {
    check_null_ptr!(statement, "Null pointer passed into statement_params_types()");
    let statement = unsafe { &*statement };
    statement.params_types.as_ptr()
}

/// Get the number of parameters from a `Statement`
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn statement_params_count(statement: *const Statement) -> c_int {
    if statement.is_null() {
        update_last_error(CoreError::InternalError {
            message: "Null pointer passed into statement_params_count()".to_string(),
        });
        return -1;
    }
    let statement = unsafe { &*statement };
    statement.params_values.len() as c_int
}

///
/// # Safety
#[no_mangle]
pub unsafe extern "C" fn statement_free(statement: *mut Statement) {
    if !statement.is_null() {
        unsafe {
            drop(Box::from_raw(statement));
        }
    }
}

#[no_mangle]
/// Free the memory associated with a `DbSchema`.
///
/// # Safety
/// # Parameters
/// - `schema` - A pointer to the `DbSchema` to free.
pub unsafe extern "C" fn db_schema_free(schema: *mut DbSchema) {
    if !schema.is_null() {
        unsafe {
            drop(Box::from_raw(schema));
        }
    }
}

/// Create a new `DbSchema` from a JSON string.
/// # Safety
/// This function is marked unsafe because it dereferences raw pointers however
/// we are careful to check for null pointers before dereferencing them.
/// # Parameters
/// - `db_type` - The type of database this schema is for.
///   Currently supported types are "postgresql", "clickhouse", "sqlite", and "mysql".
/// - `db_schema_json` - The JSON string representing the schema.
/// # Returns
/// A pointer to the newly created `DbSchemaOwned` or a null pointer if an error occurred.
#[no_mangle]
pub unsafe extern "C" fn db_schema_new(db_type: *const c_char, db_schema_json: *const c_char) -> *mut DbSchema {
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

    let db_schema = DbSchema::try_new(db_type_str.to_string(), db_schema_json, |data_ref| {
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
pub unsafe extern "C" fn last_error_message(buffer: *mut c_char, length: c_int) -> c_int {
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
pub extern "C" fn last_error_length() -> c_int {
    LAST_ERROR.with(|prev| match *prev.borrow() {
        Some(ref err) => err.to_string().len() as c_int + 1,
        None => 0,
    })
}
