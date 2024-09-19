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
//! Test suite for the Web and headless browsers.

#![cfg(target_arch = "wasm32")]

extern crate wasm_bindgen_test;
use wasm_bindgen_test::*;
use serde_wasm_bindgen::from_value as from_js_value;
use serde_wasm_bindgen::to_value as to_js_value;
use serde_json::json;

// wasm_bindgen_test_configure!(run_in_browser);
fn vs(v: Vec<(&str, &str)>) -> Vec<(String, String)> {
    v.into_iter().map(|(s, s2)| (s.to_string(), s2.to_string())).collect()
}

#[wasm_bindgen_test]
fn pass() {
    assert_eq!(1 + 1, 2);
}

#[wasm_bindgen_test]
fn js_value_conversion() {
    assert_eq!(vs(vec![("a", "1"), ("b", "2")]), from_js_value::<Vec<(String, String)>>(to_js_value(&json!({"a":"1","b":"2"})).unwrap()).unwrap());

    assert_eq!(
        vs(vec![("a", "1"), ("b", "2")]),
        from_js_value::<Vec<(String, String)>>(to_js_value(&json!([["a", "1"], ["b", "2"]])).unwrap()).unwrap()
    );
}
