#[macro_export]
macro_rules! check_null_ptr {
    ($ptr:expr, $msg:expr) => {
        if $ptr.is_null() {
            update_last_error(CoreError::InternalError { message: $msg.to_string() });
            return ptr::null_mut();
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
use crate::ffi::Tuple;
// Function to convert an array of Tuple structs to Vec<(&str, &str)>
pub fn tuples_to_vec<'a>(tuples_ptr: *const Tuple, length: usize) -> Result<Vec<(&'a str, &'a str)>, &'a str> {
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
pub fn parameters_to_tuples<'a>(_db_type: &'a str, parameters: Vec<&'a (dyn ToParam + Sync)>) -> Vec<(&'a str, &'a str)> {
    parameters
        .iter()
        .map(|p| {
            let param = match p.to_param() {
                Param::SV(SingleVal(v, Some(Cow::Borrowed("integer")))) => v,
                Param::SV(SingleVal(v, _)) => v,
                Param::PL(Payload(v, _)) => v,
                Param::Str(v) => v,
                Param::StrOwned(v) => v,
                // Param::LV(ListVal(v, _)) => match db_type {
                //     "sqlite" | "mysql" => serde_json::to_string(v).unwrap_or_default(),
                //     //_ => v,
                //     // turn it into array literal for postgres
                //     _ => format!("'{{{}}}'", v.join(", ")),
                // },
                _ => todo!("unimplemented parameter type Param::LV"),
            };
            let data_type = p.to_data_type();
            // (
            //     CString::new(param).unwrap().into_raw() as *const c_char,
            //     CString::new(data_type.as_ref().unwrap_or(&Cow::Borrowed("unknown")).as_ref()).unwrap().into_raw() as *const c_char
            // )
            (param, data_type.as_ref().unwrap_or(&Cow::Borrowed("unknown")).as_ref())
        })
        .collect::<Vec<_>>()
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
