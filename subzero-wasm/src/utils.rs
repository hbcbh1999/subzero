use wasm_bindgen::{prelude::*};
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

    // The `console.log` is quite polymorphic, so we can bind it with multiple
    // signatures. Note that we need to use `js_name` to ensure we always call
    // `log` in JS.
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    pub fn log_u32(a: u32);

    // Multiple arguments too!
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    pub fn log_many(a: &str, b: &str);
}
#[allow(unused_macros)]
macro_rules! console_log {
    // Note that this is using the `log` function imported above during
    // `bare_bones`
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}
#[allow(unused_imports)]
pub(super) use console_log;

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

pub fn cast_serde_err(err: serde_wasm_bindgen::Error) -> JsError { JsError::new(err.to_string().as_str()) }
pub fn clone_err_ref(err: &JsError) -> JsError { JsError::new(err.to_string()) }
