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
use wasm_bindgen::prelude::*;
use serde_wasm_bindgen::Error as JsError;

pub fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

#[wasm_bindgen]
extern "C" {
    // Use `js_namespace` here to bind `console.log(..)` instead of just
    // `log(..)`
    #[wasm_bindgen(js_namespace = console)]
    pub fn log(s: &str);

    // bind the warn function
    #[wasm_bindgen(js_namespace = console, js_name = warn)]
    pub fn js_warn(s: &str);

    #[wasm_bindgen(js_namespace = globalThis)]
    pub fn setTimeout(closure: &Closure<dyn FnMut()>, millis: u32);

    // get the current time
    #[wasm_bindgen(js_namespace = Date, js_name = now)]
    pub fn date_now() -> f64;

}
#[allow(unused_macros)]
macro_rules! console_log {
    // Note that this is using the `log` function imported above during
    // `bare_bones`
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}
#[allow(unused_imports)]
pub(super) use console_log;

#[allow(unused_macros)]
macro_rules! console_warn {
    // Note that this is using the `warn` function imported above during
    // `bare_bones`
    ($($t:tt)*) => (js_warn(&format_args!($($t)*).to_string()))
}
#[allow(unused_imports)]
pub(super) use console_warn;

pub fn cast_core_err(err: subzero_core::error::Error) -> JsError {
    // we can pass only strings between wasm and js
    // so we pass the error in json string format
    let mut err_json = err.json_body();
    if let Some(obj) = err_json.as_object_mut() {
        obj.insert("status".to_string(), serde_json::Number::from(err.status_code()).into());
    }
    JsError::new(
        serde_json::to_string(&err_json)
            .unwrap_or_else(|_| r#"{"message":"internal error","status":500}"#.to_string())
            .as_str(),
    )
}

// a function that returns a function
pub fn cast_serde_err(prefix: &str) -> impl Fn(serde_wasm_bindgen::Error) -> JsError + '_ {
    move |err: serde_wasm_bindgen::Error| JsError::new(format!("{}: {}", prefix, err))
}

// pub fn cast_serde_err(prefix: &str) => impl Fn(serde_wasm_bindgen::Error) -> JsError + 'static {
//     return fn (err: serde_wasm_bindgen::Error) -> JsError {
//         JsError::new(err.to_string().as_str())
//     }
// }

pub fn print_error_with_json_snippet(prefix: &str, json: &str, err: serde_json::Error) -> JsError {
    // Extract line and column information from the error (if available)
    let line = err.line();
    let column = err.column();

    // Split the JSON string by lines and attempt to get the offending line
    if let Some(error_line) = json.lines().nth(line - 1) {
        // Print out the offending line with some context around it
        // For a more sophisticated approach, you might highlight the exact position
        //println!("Error at line {}: {}", line, error_line);

        // Provide some additional context if possible
        if column > 0 && error_line.len() >= column {
            let start = column.saturating_sub(50); // Show 10 characters before the error if possible
            let end = (column + 50).min(error_line.len()); // Show 10 characters after, or up to the end of the line
                                                           //println!("Error near: {}", &error_line[start..end]);
            JsError::new(format!("invalid json schema: {}, Error near: {}", err, &error_line[start..end]))
        } else {
            JsError::new(format!("{}: {}", prefix, err))
        }
    } else {
        // If the line could not be found, print a generic message
        //println!("Error at line {}, but that line could not be found in the JSON string.", line);
        JsError::new(format!("{}: {}", prefix, err))
    }
}
